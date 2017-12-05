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

        def insert(column_family, key, values, opts=nil)
          ttl = opts.try(:[], :ttl)
          async = opts.try(:[], :async)

          insert_into_options = ttl ? " USING TTL #{ttl}" : ''

          insert_query = values.map do |name, value|
              "  INSERT INTO \"#{column_family}\" (#{KEY_FIELD}, #{NAME_FIELD}, #{VALUE_FIELD}) VALUES (#{escape(key, key_type(column_family))}, #{escape(name, name_type(column_family))}, #{escape(value, value_type(column_family))})#{insert_into_options}"
          end.join("\n")

          if batch_mode?
            @batched_queries << insert_query
          else
            query = "BEGIN BATCH\n"
            query << insert_query
            query << "\nAPPLY BATCH;"
            async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
          end
        end

        def get(column_family, key, *columns_options)
          opts = columns_options.pop if columns_options.last.is_a?(Hash)
          async = opts.try(:[], :async)

          columns = columns_options.flatten.compact

          query =
            if columns.size == 1
              "SELECT #{VALUE_FIELD} FROM \"#{column_family}\" WHERE #{KEY_FIELD} = #{escape(key, key_type(column_family))} AND #{NAME_FIELD} = #{escape(columns.first, name_type(column_family))}"
            else
              "SELECT #{NAME_FIELD}, #{VALUE_FIELD} FROM \"#{column_family}\" WHERE #{KEY_FIELD} = #{escape(key, key_type(column_family))}"
            end

          result = async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
          return result if async

          if columns.size == 1
            result.size > 0 ? result.first[VALUE_FIELD] : nil
          else
            data = result.inject({}) { |hsh, row| hsh[row[NAME_FIELD]] = row[VALUE_FIELD]; hsh }
            columns.size > 0 ? data.slice(*columns.map(&:to_s)) : data
          end
        end

        def get_columns(column_family, key, columns, opts)
          async = opts.try(:[], :async)

          name_fields = columns.map { |c| escape(c, name_type(column_family)) }.join(', ')

          query = "SELECT #{NAME_FIELD}, #{VALUE_FIELD} FROM \"#{column_family}\" WHERE #{NAME_FIELD} IN(#{name_fields}) AND #{KEY_FIELD} = #{escape(key, key_type(column_family))}"

          result = async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
          return result if async

          data = result
                 .inject({}) { |hsh, row| hsh[row[NAME_FIELD]] = row[VALUE_FIELD]; hsh }

          columns.map { |column| data[column.to_s] }
        end

        def get_columns_as_hash(column_family, key, columns, opts)
          async = opts.try(:[], :async)

          name_fields = columns.map { |c| "'#{c}'" }.join(', ')

          query = "SELECT #{NAME_FIELD}, #{VALUE_FIELD} FROM \"#{column_family}\" WHERE #{NAME_FIELD} IN(#{name_fields}) AND #{KEY_FIELD} = #{escape(key, key_type(column_family))}"

          result = async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
          return result if async

          result
            .inject({}) { |hsh, row| hsh[row[NAME_FIELD]] = row[VALUE_FIELD]; hsh }
        end

        def get_value(column_family, key, column, consistency)
          get column_family, key, [column], :consistency => consistency
        end

        def add(column_family, key, by, fields, opts=nil)
          async = opts.try(:[], :async)
          fields = [fields] unless fields.is_a?(Array)

          fields.each do |field|
            query = "UPDATE \"#{column_family}\" SET #{VALUE_FIELD} = #{VALUE_FIELD} + #{by} WHERE #{KEY_FIELD} = #{escape(key, key_type(column_family))} AND #{NAME_FIELD} = #{escape(field, name_type(column_family))};"
            async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
          end
        end

        def add_multiple_columns(column_family, key, hash, opts=nil)
          async = opts.try(:[], :async)
          fields = [fields] unless fields.is_a?(Array)

          hash.each do |field, by|
            query = "UPDATE \"#{column_family}\" SET #{VALUE_FIELD} = #{VALUE_FIELD} + #{by} WHERE #{KEY_FIELD} = #{escape(key, key_type(column_family))} AND #{NAME_FIELD} = #{escape(field, name_type(column_family))};"
            async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
          end
        end

        def remove(column_family, key, *args)
          opts = args.pop if args.last.is_a?(Hash)
          async = opts.try(:[], :async)

          query =
            if args.first.nil? || args.first.is_a?(Hash)
              "DELETE FROM \"#{column_family}\" WHERE #{KEY_FIELD} = #{escape(key, key_type(column_family))};"
            else
              "DELETE \"#{column_family}\" WHERE #{KEY_FIELD} = #{escape(key, key_type(column_family))} AND #{NAME_FIELD} = #{escape(args.first, name_type(column_family))};"
            end

          async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
        end

        def get_range(column_family, opts={}, &blk)
          key_count = opts[:key_count] || 100
          query = "SELECT #{KEY_FIELD} FROM \"#{column_family}\" LIMIT #{key_count}"
          keys = self.execute(query, execute_options(opts)).map { |result| result[KEY_FIELD] }
          keys.size > 0 ? multi_get(column_family, keys) : {}
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

        def get_slice(column_family, key, column, start, finish, count, reversed, consistency, opts={})
          opts[:consistency] = consistency

          query = "SELECT * FROM \"#{column_family}\" WHERE #{KEY_FIELD} = #{escape(key, key_type(column_family))}"
          query << " AND #{NAME_FIELD} = #{escape(column, name_type(column_family))}" if column
          query << " AND #{NAME_FIELD} >= #{escape(start, name_type(column_family))}" unless start.empty?
          query << " AND #{NAME_FIELD} <= #{escape(finish, name_type(column_family))}" unless finish.empty?
          query << " ORDER BY #{NAME_FIELD} #{reverse_comparator(column_family) ? 'ASC' : 'DESC'}" if reversed
          query << " LIMIT #{count}"

          self.execute(query, execute_options(opts)).inject({}) do |results, row|
            results[decode(row[NAME_FIELD], name_type(column_family))] = decode(row[VALUE_FIELD], value_type(column_family))
            results
          end
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
          self.cluster.keyspace(keyspace).table(column_family).column(KEY_FIELD).type.kind
        end

        def name_type(column_family)
          self.cluster.keyspace(keyspace).table(column_family).column(NAME_FIELD).type.kind
        end

        def value_type(column_family)
          self.cluster.keyspace(keyspace).table(column_family).column(VALUE_FIELD).type.kind
        end

        def reverse_comparator(column_family)
          self.cluster.keyspace(keyspace).table(column_family).send(:clustering_order).first == :desc
        end

        def escape(str, type)
          case type
          when :timeuuid
            convert_str_to_timeuuid str
          when :blob
            convert_str_to_hex str
          else
            "'#{str}'"
          end
        end

        def decode(val, type)
          case type
          when :timeuuid
            SimpleUUID::UUID.new(val.to_s).to_s # binary version
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
      end
    end
  end
end
