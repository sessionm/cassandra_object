module CassandraObject
  module Adapters
    class CassandraDriver
      CLUSTER_CONFIG_OPTIONS = [
        :credentials, :auth_provider, :compression, :hosts, :logger, :port,
        :load_balancing_policy, :reconnection_policy, :retry_policy, :listeners,
        :consistency, :trace, :page_size, :compressor, :username, :password,
        :ssl, :server_cert, :client_cert, :private_key, :passphrase,
        :connect_timeout, :futures_factory, :datacenter, :address_resolution,
        :address_resolution_policy, :idle_timeout, :heartbeat_interval, :timeout,
        :synchronize_schema, :schema_refresh_delay, :schema_refresh_timeout,
        :shuffle_replicas, :client_timestamps
      ]

      attr_reader :config

      def initialize(config)
        @config = config
      end

      def cluster
        @cluster ||= Cassandra.cluster cluster_config
      end

      def client
        @client ||= self.new_client
      end

      def new_client
        Client.new(cluster.connect(config[:keyspace]), cluster)
      end

      def close
        @client.try(:close)
        @client = nil
      end

      def cluster_config
        {
          :hosts => config[:servers].map { |server| server.sub /:\d+/, '' },
          :port => config[:port] || 9042,
          :connect_timeout => config[:thrift][:connect_timeout] || 10,
          :timeout => config[:thrift][:timeout] || 10,
          :logger => config[:logger] || (defined?(Rails) && Rails.logger) || Logger.new(STDOUT),
          :consistency => (config[:consistency] || {})[:write_default].try(:to_sym) || :one,
        }
      end

      def cluster_config_new
        config.slice(*CLUSTER_CONFIG_OPTIONS).reverse_merge(
          :hosts => config[:servers].map { |server| server.sub /:\d+/, '' },
          :port => 9042,
          :connect_timeout => config[:thrift].try(:[], :connect_timeout) || 10,
          :timeout => config[:thrift].try(:[], :timeout) || 10,
          :logger => config[:logger] || (defined?(Rails) && Rails.logger) || Logger.new(STDOUT)
        ).merge(
          :consistency => (config[:consistency] || {})[:write_default].try(:to_sym) || :one
        )
      end

      class SchemaCache < ActiveRecord::ConnectionAdapters::SchemaCache
        def columns_hash(table_name)
          @columns_hash[table_name] ||= {'id' => 'id'}
        end
      end

      # The client class acts like the old cassandra gem
      class Client < ActiveRecord::ConnectionAdapters::AbstractAdapter
        attr_reader :session, :cluster

        KEY_FIELD = 'key'
        NAME_FIELD = 'column1'
        VALUE_FIELD = 'value'

        def initialize(session, cluster)
          @session = session
          @cluster = cluster
        end

        def close
          session.close
        end

        def each(column_family, options = {})
          get_range(column_family, options) do |key, columns|
            yield key, columns
          end
        end

        def execute(query, options={})
          ActiveSupport::Notifications.instrument('query.cassandra', query: query, options: options, async: false) do
            session.execute query, options
          end
        end

        def execute_async(query, options={})
          ActiveSupport::Notifications.instrument('query.cassandra', query: query, options: options, async: true) do
            session.execute_async query, options
          end
        end

        def cqlsh_options(opts, opt_list = [:ttl, :timestamp])

          return nil unless opts

          ol = opt_list.map{|o| o = opts.try(:[], o) ? "#{o.to_s.upcase} #{opts[o]}": nil}.compact
          ol.count > 0 ? "USING #{ol.join(' AND ')}" : nil
        end

        def exists?(column_family, key, *columns_options)
          opts = columns_options.pop if columns_options.last.is_a?(Hash)

          columns = columns_options.flatten.compact || []

          result = get( column_family, key, columns, opts)
          ![{}, nil].include?(result)
        end

        def insert(column_family, key, values, opts=nil)
          async = opts.try(:[], :async)
          insert_into_options = cqlsh_options(opts)

          key_fields = get_key_fields(column_family)
          column_fields = get_column_fields(column_family)

          key_parts = get_parts(column_family, key, key_fields)
          insert_query = values.map do |name, value|
            column_parts = get_parts(column_family, name, column_fields)
            "  INSERT INTO \"#{column_family}\" (#{key_fields.map(&:first).join(', ')}, #{column_fields.map(&:first).join(', ')}, #{VALUE_FIELD}) VALUES (#{key_parts.map { |f, v| escape(v, get_type(column_family, f)) }.join(', ')}, #{column_parts.map { |f, v| escape(v, get_type(column_family, f)) }.join(', ')}, #{escape(value, value_type(column_family))}) #{insert_into_options}"
          end.join("\n")

          if batch_mode?
            @batched_queries << insert_query
          else
            query = "BEGIN BATCH\n"
            query << insert_query
            query << "\nAPPLY BATCH;"
            ret = async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
          end
          ret
        end

        def get(column_family, key, *columns_options)
          # puts "Cassandra driver CF=#{column_family} key=#{key} opts=#{columns_options.inspect}"
          opts = columns_options.pop if columns_options.last.is_a?(Hash)
          async = opts.try(:[], :async)

          columns = columns_options.flatten.compact
          column_fields = get_column_fields(column_family)

          query =
            if columns.size == 1
              "SELECT writetime(#{VALUE_FIELD}), #{VALUE_FIELD} FROM \"#{column_family}\" WHERE #{key_clause(column_family, key)} AND #{column_clause(column_family, columns.first)}"
            else
              "SELECT writetime(#{VALUE_FIELD}), #{column_fields.map(&:first).join(', ')}, #{VALUE_FIELD} FROM \"#{column_family}\" WHERE #{key_clause(column_family, key)}"
            end
          result = async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
          return result if async


          # strange mixed model for get returns a string for one column retrieval,
          # use an early exit and no cassandra ordered hash
          if columns.size == 1
            # data.[]= columns[0], result.first[VALUE_FIELD], result.first["writetime(#{VALUE_FIELD})"]
            return result.first[VALUE_FIELD] if result.size > 0
            return nil
          end

          data = ::CassandraObject::OrderedHash.new
          result.rows.each {|row| data.[]= column_string(row, column_fields), row[VALUE_FIELD], row["writetime(#{VALUE_FIELD})"] }
          data.slice(*columns.map(&:to_s)) if columns.size > 0
          data
        end

        def get_columns(column_family, key, *columns_options)
          opts = columns_options.pop if columns_options.last.is_a?(Hash)
          columns = columns_options.flatten.compact
          async = opts.try(:[], :async)

          name_fields = columns.map { |c| escape(c, name_type(column_family)) }.join(', ')

          query = "SELECT #{NAME_FIELD}, #{VALUE_FIELD} FROM \"#{column_family}\" WHERE #{NAME_FIELD} IN(#{name_fields}) AND #{key_clause(column_family, key)};"

          result = async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
          return result if async

          data = result
                 .inject({}) { |hsh, row| hsh[row[NAME_FIELD]] = row[VALUE_FIELD]; hsh }

          columns.map { |column| data[column.to_s] }
        end



        def get_columns_as_hash(column_family, key, columns, opts)
          async = opts.try(:[], :async)
          col_fields = get_column_fields(column_family)
          cols = columns.count == 1 ? columns.first : columns if columns.is_a? Array
          cols ||= columns

          if col_fields.count == 1  # single column field, but one or more column values
            field_list = cols.is_a?(Array) ? "'#{cols.join("','")}'" : "'#{cols}'"
            col_list = col_fields.map{|c| c[0]}.join(",")
            where_clause = " #{col_list} in (#{field_list}) "
          else # multiple column fields, multiple column values
            where_clause = " #{column_clause(column_family, cols)} "
          end

          query = "SELECT writetime(#{VALUE_FIELD}), #{NAME_FIELD}, #{VALUE_FIELD} FROM \"#{column_family}\" WHERE #{where_clause} AND #{key_clause(column_family, key)};"

          result = async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
          return result if async

          #result
          #  .inject({}) { |hsh, row| hsh[row[NAME_FIELD]] = row[VALUE_FIELD]; hsh }
          data = ::CassandraObject::OrderedHash.new
          if col_fields.count == 1
            result.rows.each {|row| data.[]= row[NAME_FIELD], row[VALUE_FIELD], row["writetime(#{VALUE_FIELD})"] }
          else
            result.rows.each {|row| data.[]= Cassandra::Composite.new(cols), row[VALUE_FIELD], row["writetime(#{VALUE_FIELD})"] }
          end
          data
        end

        def get_value(column_family, key, column, consistency)
          get column_family, key, [column], :consistency => consistency
        end

        def add(column_family, key, by, fields, opts=nil)
          async = opts.try(:[], :async)
          fields = [fields] unless fields.is_a?(Array)

          fields.each do |field|
            query = "UPDATE \"#{column_family}\" SET #{VALUE_FIELD} = #{VALUE_FIELD} + #{by} WHERE #{key_clause(column_family, key)} AND #{column_clause(column_family, field)};"
            async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
          end
        end

        def add_multiple_columns(column_family, key, hash, opts=nil)
          async = opts.try(:[], :async)
          fields = [fields] unless fields.is_a?(Array)

          hash.each do |field, by|
            query = "UPDATE \"#{column_family}\" SET #{VALUE_FIELD} = #{VALUE_FIELD} + #{by} WHERE #{key_clause(column_family, key)} AND #{column_clause(column_family, field)};"
            async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
          end
        end

        def remove(column_family, key, *args)
          opts = args.pop if args.last.is_a?(Hash)
          async = opts.try(:[], :async)
          delete_options = cqlsh_options(opts, [:timestamp])
          query =
            if args.first.nil? || args.first.is_a?(Hash)
              "DELETE FROM \"#{column_family}\" #{delete_options} WHERE #{key_clause(column_family, key)};"
            else
              "DELETE FROM \"#{column_family}\" #{delete_options} WHERE #{key_clause(column_family, key)} AND #{column_clause(column_family, args.first)};"
            end

          async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
        end

        def truncate(column_family, *args)
          self.truncate!(column_family, *args)
        end

        def truncate!(column_family, *args)
          opts = args.pop if args.last.is_a?(Hash)
          async = opts.try(:[], :async)
          query = "TRUNCATE \"#{column_family}\" "

          async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
        end

        def count_range(column_family, options = {})
          get_range_keys(column_family, options).length
        end

        def get_range_keys(column_family, options = {})
          get_range(column_family,options.merge!(:count => 1)).keys
        end

        def get_range(column_family, opts={}, &blk)
          key_count = opts[:key_count] || 100
          query = "SELECT #{KEY_FIELD} FROM \"#{column_family}\" LIMIT #{key_count}"
          keys = self.execute(query, execute_options(opts)).map { |result| result[KEY_FIELD] }
          results = keys.size > 0 ? multi_get(column_family, keys) : {}
          return results unless block_given?
          results.each do |key, columns|
            yield key, columns
          end
        end

        def multi_get(column_family, keys, *args)
          opts = args.pop if args.last.is_a?(Hash)
          keys = keys.map { |key| escape(key, key_type(column_family)) }.join(',')
          results = {}
          query = "SELECT * FROM \"#{column_family}\" WHERE #{KEY_FIELD} IN(#{keys})"
          self.execute(query, execute_options(opts)).each do |row|
            results[row[KEY_FIELD]] ||= {}
            results[row[KEY_FIELD]][row[NAME_FIELD]] = row[VALUE_FIELD]
          end
          results
        end

        def multi_get_columns(column_family, key_vals, cols, *args)
          opts = args.pop if args.last.is_a?(Hash)
          keys =  key_vals.map {|k| escape(k, key_type(column_family)) }.join(',')
          column_fields = get_column_fields(column_family)
          results = CassandraObject::OrderedHash.new

          # ----- temporary DB dump for diagnostics
          # puts "------ cf_data_migration=#{SystemConfig.cf_data_migration}"
          # puts "------ key_vals=#{key_vals} cols=#{cols}"
          # puts "------ keys    =#{keys}"
          # query = "SELECT * FROM \"#{column_family}\""
          # self.execute(query).each do |row|
          #   puts "------ key=#{decode(row['key'], key_type(column_family))} col1=#{row['column1']} val=#{decode(row[VALUE_FIELD], value_type(column_family))}"
          # end
          # -----

          query = "SELECT writetime(#{VALUE_FIELD}), #{VALUE_FIELD}, key, #{column_fields.map(&:first).join(',')} FROM \"#{column_family}\" WHERE #{KEY_FIELD} IN(#{keys})"
          self.execute(query, execute_options(opts)).each do |row|
            if !column_fields.map(&:first).select{ |col| cols.include?(row[col])}.empty?
              if results[row['key']]
                results[decode(row['key'], key_type(column_family))] << decode(row[VALUE_FIELD], value_type(column_family))
              else
                results.[]= decode(row['key'], key_type(column_family)), [decode(row[VALUE_FIELD], value_type(column_family))]
              end
            end
          end
          results
        end

        def get_slice_by_token(column_family, primary_columns, start, finish, count, reversed, consistency, opts={})
          # primary columns is a hash of key => value to build the token string
          #   The last hash field MUST be the field used to select the slice and the value is ignored.
          #   Supply either or both start and finish for the last field's range values, for example:
          # get_slice_by_token( 'UserPointsTransactions', {key: 'users uuid', column1: nil }, date1_to_uuid, date2_to_uuid , 50, true, :any )
          #
          opts[:consistency] = consistency
          raise "Array of primary key fields required in primary_columns argument " unless primary_columns.kind_of?(Hash)
          raise "Either (or both) start and finish field names is required " unless start or finish
          #primary_keys = "token(#{primary_columns.join(', ')}) "

          column_fields = get_column_fields(column_family)

          # we need to use the token function on ranged select
          query = "SELECT writetime(#{VALUE_FIELD}, #{column_fields.map(&:first).join(', ')}, #{VALUE_FIELD} FROM \"#{column_family}\" WHERE"
          query << " #{token_clause(column_family, primary_columns, '>=', start)} AND" unless start.nil? || start.blank?
          query << " #{token_clause(column_family, primary_columns, '<=', finish)} AND" unless finish.nil? || finish.blank?
          query = query[0..-4]
          if reversed
            direction = reverse_comparator(column_family) ? 'ASC' : 'DESC'
            query << " ORDER BY "
            query << column_fields.map { |f, _| "#{f} #{direction}" }.join(', ')
          end
          query << " LIMIT #{count}"
          results = CassandraObject::OrderedHash.new
          self.execute(query, execute_options(opts)).each do |row|
            results.[]= column_string(row, column_fields), decode(row[VALUE_FIELD], value_type(column_family)), row["writetime(#{VALUE_FIELD})"]
          end
          results
        end

        def token_clause( column_family, primary_columns, operator, val )
          tc = ''
          vl = ''
          # we want the last key field in the hash.  This is a short hash (maybe one element)
          last_key = nil
          primary_columns.each { |k,v| last_key = k}
          last_key = last_key.to_sym

          column_fields = get_column_fields(column_family)
          primary_columns.each do |k,v|
            type = nil
            # find the column type.  If no type, then this is the key field
            column_fields.each { |a| a[0] == k.to_s ? type = a[1] : nil }
            tc = tc + "#{k.to_s}, "
            if type
              vl = vl + "#{escape(v,type)}, " unless k.to_sym == last_key
              vl = vl + "#{escape(val,type)}, " if k.to_sym == last_key
            else # key field
              vl = vl + "#{escape(v,key_type(column_family))}, "
            end
          end

          #"#{field} #{operator} #{escape(val, type)}"
          "token(#{tc[0..-3]}) #{operator} token(#{vl[0..-3]})"
        end


        def get_slice(column_family, key, column, start, finish, count, reversed, consistency, opts={})
          opts[:consistency] = consistency

          cql_start = start
          cql_finish = finish
          column_fields = get_column_fields(column_family)

          # screen out nil/null start or finish values
          # if !(start.blank? || finish.blank?) &&
          #     column_fields[0][1] == :timeuuid &&
          #     SimpleUUID::UUID.new(start) > SimpleUUID::UUID.new(finish)
          #   cql_start = finish
          #   cql_finish = start
          # end

          # we are using CQL maxTimeUUID and minTimeUUID which operate in the opposite order from thrift
          if column_fields[0][1] == :timeuuid && (!cql_start.blank? || !cql_finish.blank?)
            if !cql_start.blank? && !cql_finish.blank?  # both not blank
              cql_start = finish
              cql_finish = start
            elsif cql_finish.blank?  # finish is blank
              cql_finish = start
              cql_start = SimpleUUID::UUID.new( 100.years.ago.utc ) # assume a long time ago
            elsif cql_start.blank?  # start is blank
              cql_finish = SimpleUUID::UUID.new( Time.now.utc )  # assume now
              cql_start = finish
            end
            # puts "------ get slice start= #{SimpleUUID::UUID.new(cql_start).to_time unless cql_start.blank?}"
            # puts "------ get slice finish= #{SimpleUUID::UUID.new(cql_finish).to_time unless cql_finish.blank?}"
          end

          query = "SELECT writetime(#{VALUE_FIELD}), #{column_fields.map(&:first).join(', ')}, #{VALUE_FIELD} FROM \"#{column_family}\" WHERE #{key_clause(column_family, key)}"
          query << " AND #{column_clause(column_family, column)}" if column
          query << " AND #{column_clause(column_family, cql_start, '>=' )}" unless cql_start.blank?
          query << " AND #{column_clause(column_family, cql_finish, '<=')}" unless cql_finish.blank?
          if reversed
            direction = reverse_comparator(column_family) ? 'ASC' : 'DESC'
            query << " ORDER BY "
            query << column_fields.map { |f, _| "#{f} #{direction}" }.join(', ')
          end
          query << " LIMIT #{count}"
          # puts "--------query = #{query}"
          # self.execute(query, execute_options(opts)).inject({}) do |results, row|
          #   results[column_string(row, column_fields)] = decode(row[VALUE_FIELD], value_type(column_family))
          #   results
          # end

          results = ::CassandraObject::OrderedHash.new
          self.execute(query, execute_options(opts)).each do |row|
            results.[]= column_string(row, column_fields), decode(row[VALUE_FIELD], value_type(column_family)), row["writetime(#{VALUE_FIELD})"]
          end
          results
        end

        def execute_options(opts)
          opts.try(:slice,
                   :consistency,
                   :page_size,
                   :trace,
                   :timeout,
                   :serial_consistency
                  ) || {}
        end

        def keyspace
          session.keyspace
        end

        def has_table?(name)
          self.cluster.keyspace(session.keyspace).has_table? name
        end

        def schema_cache
          @schema_cache ||= SchemaCache.new(self)
        end

        def column_families
          @column_families ||= self.cluster.keyspace(session.keyspace).tables.inject({}) { |hsh, table| hsh[table.name] = table; hsh }
        end

        def batch_mode?
          !! @batched_queries
        end

        def batch(opts=false)
          if @batched_queries
            yield
          else
            begin
              async = opts.try(:[], :async)
              @batched_queries ||= []
              yield
              query = "BEGIN BATCH\n"
              query << @batched_queries.join("\n")
              query << "\nAPPLY BATCH;"
              async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
            rescue Exception => e
              raise e
            ensure
              @batched_queries = nil
            end
          end
        end

        def schema(reload=false)
          if reload
            remove_instance_variable(:@schema_cache) if instance_variable_defined?(:@schema_cache)
            remove_instance_variable(:@column_families) if instance_variable_defined?(:@column_families)
            self.cluster.refresh_schema
          end
        end

        def add_column_family(column_family)
          value_type = column_family.default_validation_class == 'CounterColumnType' ? 'counter' : 'text'

          query = <<-CQL
CREATE TABLE "#{column_family.name}" (
  key blob,
  column1 text,
  value #{value_type},
  PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
  AND CLUSTERING ORDER BY (column1 ASC)
  AND bloom_filter_fp_chance = 0.1
  AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
  AND comment = ''
  AND compaction = {'sstable_size_in_mb': '160', 'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
  AND dclocal_read_repair_chance = 0.1
  AND default_time_to_live = 0
  AND gc_grace_seconds = 864000
  AND max_index_interval = 2048
  AND memtable_flush_period_in_ms = 0
  AND min_index_interval = 128
  AND read_repair_chance = 0.0
  AND speculative_retry = 'NONE';
CQL

          self.execute(query)
          self.schema(true)
        end

        def key_type(column_family)
          get_type(column_family, KEY_FIELD)
        end

        def name_type(column_family)
          get_type(column_family, NAME_FIELD)
        end

        def value_type(column_family)
          get_type(column_family, VALUE_FIELD)
        end

        def get_type(column_family, field)
          cluster.keyspace(keyspace).table(column_family).column(field).type.kind
        end

        def get_key_fields(column_family)
          cluster.keyspace(keyspace).table(column_family).columns.select { |c| c.name =~ /^key/ }.map { |c| [c.name, c.type.kind] }
        end

        def get_column_fields(column_family)
          cluster.keyspace(keyspace).table(column_family).columns.select { |c| c.name =~ /^column/ }.map { |c| [c.name, c.type.kind] }
        end

        def reverse_comparator(column_family)
          self.cluster.keyspace(keyspace).table(column_family).send(:clustering_order).first == :desc
        end

        def get_parts(column_family, val, fields)
          val =
            if fields.size > 1
              Cassandra::Composite.new_from_packed(val.dup).parts
            else
              [val]
            end

          fields.each_with_index.map { |(field, type), idx| [field, normalize_composite_key_part(val[idx], type), type] }
        end

        def key_clause(column_family, val)
          clause(column_family, val, get_key_fields(column_family))
        end

        def column_clause(column_family, val, operator='=')
          clause(column_family, val, get_column_fields(column_family), operator)
        end

        def clause(column_family, val, fields, operator='=')
          if operator == '='
            get_parts(column_family, val, fields).map { |field, val, type| "#{field} #{operator} #{escape(val, type)}" }.join(' AND ')
          else
            field, val, type = get_parts(column_family, val, fields).first
            "#{field} #{operator} #{escape(val, type, operator)}"
          end
        end

        def escape(str, type, operator = nil)
          case type
          when :timeuuid
            # in CQL, we need to use the maxTimeuuid and minTimeuud functions
            # also ... need to adjust endpoint by 1 second down for max and 1 second up for min because
            # these are not exact matches on the endpoints (UUID varies), however SimpleUUID endpoints have 1 sec resolution
            case operator
            when '>'
              "maxTimeuuid('#{SimpleUUID::UUID.new(str).to_time-1}')"  # we have to adjust by 1 sec, endpoint not exact
            when '<'
              "minTimeuuid('#{SimpleUUID::UUID.new(str).to_time+1}')"  # we have to adjust by 1 sec, endpoint not exact
            else
              convert_str_to_timeuuid str
            end
          when :blob
            convert_str_to_hex str
          when :int, :bigint
            str
          when :string, :varchar
            "'#{string_esc_quote(str)}'"
          else
            "'#{str}'"
          end
        end

        def string_esc_quote(arg)
          case arg
          when String
            arg.gsub(/'/, {"'"=>"''"})
          when Array
            arg.map {|m| string_esc_quote(m)}
          when Hash, ActiveSupport::HashWithIndifferentAccess
            new_hash = arg.class.new
            arg.each {|k,v| new_hash[k] = string_esc_quote(v)}
            new_hash
          else
            arg
          end
        end

        def normalize_composite_key_part(val, type)
          case type
          when :timeuuid
            return if val.blank?
            SimpleUUID::UUID.new(val).to_s
          when :int
            val.unpack('N').first
          when :bigint
            Cassandra::Long.new(val).to_i
          else
            val
          end
        end

        def decode(val, type)
          case type
          when :timeuuid
            SimpleUUID::UUID.new(val.to_s).to_s # binary version
          when :int, :float, :bigint
            val.to_s
          else
            val
          end
        end

        # when the column names are timeuuid
        def convert_str_to_timeuuid(str)
          SimpleUUID::UUID.new(str).to_guid
        end

        # insert a blob
        def convert_str_to_hex(str)
          '0x' << str.unpack('H*').first
        end

        def column_string(row, fields)
          if fields.size > 1
            parts = fields.map { |f, t| decode(row[f], t) }
            Cassandra::Composite.new(*parts).to_s
          elsif row[NAME_FIELD].is_a?(::Cassandra::Uuid)
            row[NAME_FIELD].to_s
          else
            row[NAME_FIELD]
          end
        end
      end
    end
  end
end
