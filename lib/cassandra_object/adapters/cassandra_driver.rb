module CassandraObject
  module Adapters
    class CassandraDriver
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

        def execute(*args)
          session.execute *args
        end

        def execute_async(*args)
          session.execute_async *args
        end

        def insert(column_family, key, values, opts=nil)
          ttl = opts.try(:[], :ttl)
          async = opts.try(:[], :async)

          insert_into_options = ttl ? " USING TTL #{ttl}" : ''
          key = "textAsBlob('#{key}')"

          query = "BEGIN BATCH\n"
          query << values.map do |name, value|
            "  INSERT INTO \"#{column_family}\" (#{KEY_FIELD}, #{NAME_FIELD}, #{VALUE_FIELD}) VALUES (#{key}, #{escape(name, opts.try(:[], :name_type))}, #{escape(value, opts.try(:[], :value_type))})#{insert_into_options}"
          end.join("\n")
          query << "\nAPPLY BATCH;"

          async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
        end

        def get(column_family, key, *columns_options)
          opts = columns_options.pop if columns_options.last.is_a?(Hash)
          async = opts.try(:[], :async)

          columns = columns_options.flatten.compact

          key = "textAsBlob('#{key}')"

          query =
            if columns.size == 1
              "SELECT #{VALUE_FIELD} FROM \"#{column_family}\" WHERE #{KEY_FIELD} = #{key} AND #{NAME_FIELD} = '#{columns.first}'"
            else
              "SELECT #{NAME_FIELD}, #{VALUE_FIELD} FROM \"#{column_family}\" WHERE #{KEY_FIELD} = #{key}"
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

          key = "textAsBlob('#{key}')"

          name_fields = columns.map { |c| "'#{c}'" }.join(', ')

          query = "SELECT #{NAME_FIELD}, #{VALUE_FIELD} FROM \"#{column_family}\" WHERE #{NAME_FIELD} IN(#{name_fields}) AND #{KEY_FIELD} = #{key}"

          result = async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
          return result if async

          data = result
                 .inject({}) { |hsh, row| hsh[row[NAME_FIELD]] = row[VALUE_FIELD]; hsh }

          columns.map { |column| data[column.to_s] }
        end

        def add(column_family, key, by, fields, opts=nil)
          async = opts.try(:[], :async)
          fields = [fields] unless fields.is_a?(Array)
          key = "textAsBlob('#{key}')"

          fields.each do |field|
            query = "UPDATE \"#{column_family}\" SET #{VALUE_FIELD} = #{VALUE_FIELD} + #{by} WHERE #{KEY_FIELD} = #{key} AND #{NAME_FIELD} = '#{field}';"
            async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
          end
        end

        def remove(column_family, key, *args)
          opts = args.pop if args.last.is_a?(Hash)
          async = opts.try(:[], :async)
          key = "textAsBlob('#{key}')"

          query =
            if args.first.nil? || args.first.is_a?(Hash)
              "DELETE FROM \"#{column_family}\" WHERE #{KEY_FIELD} = #{key};"
            else
              "DELETE \"#{column_family}\" WHERE #{KEY_FIELD} = #{key} AND #{NAME_FIELD} = '#{args.first}';"
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
          keys = keys.map { |key| "textAsBlob('#{key}')" }.join(',')
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

          key = "textAsBlob('#{key}')"

          query = "SELECT * FROM \"#{column_family}\" WHERE #{KEY_FIELD} = #{key}"
          query << " AND #{NAME_FIELD} = #{escape(column, opts[:name_type])}" if column
          query << " AND #{NAME_FIELD} >= #{escape(start, opts[:name_type])}" unless start.empty?
          query << " AND #{NAME_FIELD} <= #{escape(finish, opts[:name_type])}" unless finish.empty?
          query << " ORDER BY #{NAME_FIELD} #{opts[:reverse_comparator] ? 'ASC' : 'DESC'}" if reversed
          query << " LIMIT #{count}"

          self.execute(query, execute_options(opts)).inject({}) do |results, row|
            results[decode(row[NAME_FIELD], opts[:name_type])] = decode(row[VALUE_FIELD], opts[:value_type])
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

        def batch
          yield
        end

        def schema(reload=false)
          if reload
            remove_instance_variable(:@schema_cache) if instance_variable_defined?(:@schema_cache)
            remove_instance_variable(:@column_families) if instance_variable_defined?(:@column_families)
          end
        end

        def add_column_family(column_family)
          value_type = column_family.column_type == 'Standard' ? 'text' : 'counter'

          query = <<-CQL
CREATE TABLE "#{column_family.name}" (
  key blob,
  column1 text,
  value #{value_type},
  PRIMARY KEY (key, column1)
)
CQL

          self.execute(query)

          self.column_families[column_family.name.to_s] = column_family
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
