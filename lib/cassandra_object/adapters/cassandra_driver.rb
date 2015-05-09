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
        Client.new(cluster.connect(config[:keyspace]))
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

      # The client class acts like the old cassandra gem
      class Client
        attr_reader :session

        KEY_FIELD = 'key'
        NAME_FIELD = 'column1'
        VALUE_FIELD = 'value'

        def initialize(session)
          @session = session
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
            "  INSERT INTO \"#{column_family}\" (#{KEY_FIELD}, #{NAME_FIELD}, #{VALUE_FIELD}) VALUES (#{key}, '#{name}', '#{value}')#{insert_into_options}"
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

        def add(column_family, key, by, field, opts=nil)
          async = opts.try(:[], :async)
          key = "textAsBlob('#{key}')"

          query = "UPDATE \"#{column_family}\" SET #{VALUE_FIELD} = #{VALUE_FIELD} + #{by} WHERE #{KEY_FIELD} = #{key} AND #{NAME_FIELD} = '#{field}';"

          async ? self.execute_async(query, execute_options(opts)) : self.execute(query, execute_options(opts))
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
      end
    end
  end
end
