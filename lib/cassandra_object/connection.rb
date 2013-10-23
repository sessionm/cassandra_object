require 'with_connection'

module CassandraObject
  module Connection
    extend ActiveSupport::Concern
    
    included do
      class_attribute :connection_spec

      class_eval do

        @@schema = nil
        def self.new_connection(async, servers)
          spec = connection_spec.dup

          if async
            require 'thrift_client/event_machine'
            spec[:thrift].merge!(:transport => Thrift::EventMachineTransport,
                                 :transport_wrapper => nil)
          end

          Cassandra.new(spec[:keyspace], servers || spec[:servers], spec[:thrift]).tap do |conn|
            conn.disable_node_auto_discovery! if spec[:disable_node_auto_discovery]
            if spec[:cache_schema]
              if @@schema
                conn.instance_variable_set '@schema', @@schema
              else
                begin
                  @@schema = conn.schema
                rescue CassandraThrift::InvalidRequestException => e
                  # initially the schema doesn't exists
                end
              end
            end
          end
        end

        def self.new_async_connection(servers=nil)
          new_connection true, servers
        end

        def self.new_sync_connection(servers=nil)
          new_connection false, servers
        end

        def self.new_async_connection_pool(servers=nil)
          adapter_method = Proc.new do
            self.new_async_connection servers
          end
          spec = ActiveRecord::Base::ConnectionSpecification.new self.connection_spec, adapter_method
          WithConnection::ConnectionPool.new "async cassandra", spec
        end

        def self.new_sync_connection_pool(servers=nil)
          adapter_method = Proc.new do
            self.new_sync_connection servers
          end
          spec = ActiveRecord::Base::ConnectionSpecification.new self.connection_spec, adapter_method
          WithConnection::ConnectionPool.new "sync cassandra", spec
        end

        @@ring = nil
        def self.ring
          @@ring ||= self.new_sync_connection.ring
        end

        def self.servers_and_ranges(datacenter)
          datacenter = datacenter.to_s
          ring.map do |t| 
            {
              :start_token => t.start_token.to_i,
              :end_token => t.end_token.to_i == 0 ? 0xffffffffffffffffffffffffffffffff : (t.end_token.to_i - 1), 
              :servers => t.endpoint_details.select { |d| d.datacenter == datacenter }.map { |d| "#{d.host}:9160" }
            }
          end
        end

        NEGATIVE_TEST  = 0x80000000000000000000000000000000
        FLIP_BITS_MASK = 0xffffffffffffffffffffffffffffffff
        def self.ranged_connection_pool_key_algo
          Proc.new do |key|
            key = Digest::MD5.hexdigest(key.to_s).to_i(16)

            # https://github.com/datastax/java-driver/blob/2.0/driver-core/src/main/java/com/datastax/driver/core/Token.java:259
            # test to see if the unsigned integer is a negative singed value
            if (key & NEGATIVE_TEST) != 0
              # need to flip all the bits
              # abs
              (key ^ FLIP_BITS_MASK) + 1
            else
              key
            end
          end
        end

        def self.new_ranged_connection_pool(async)
          if self.connection_spec[:datacenter]
            require 'with_connection/ranged_connection_pool'

            default_pool = async ? new_async_connection_pool : new_sync_connection_pool

            ranges_and_pools = self.servers_and_ranges(self.connection_spec[:datacenter]).map do |info|
              pool = async ? new_async_connection_pool(info[:servers]) : new_sync_connection_pool(info[:servers])
              [WithConnection::RangedConnectionPool::BasicRange.new(info[:start_token], info[:end_token]), pool]
            end

            ranges_and_pools.size <= 1 ? default_pool : WithConnection::RangedConnectionPool.new(ranges_and_pools, default_pool, self.ranged_connection_pool_key_algo)
          else
            async ? new_async_connection_pool : new_sync_connection_pool
          end
        end

        @@sync_connection_pool = nil
        def self.sync_connection_pool
          @@sync_connection_pool ||= new_ranged_connection_pool(false)
        end

        @@async_connection_pool = nil
        def self.async_connection_pool
          @@async_connection_pool ||= new_ranged_connection_pool(true)
        end

        if defined?(EM)
          def self.connection_pool
            EM.reactor_running? ? self.async_connection_pool : self.sync_connection_pool
          end
        else
          def self.connection_pool
            self.sync_connection_pool
          end
        end

        def connection_pool
          self.class.connection_pool
        end

        def self.connection
          self.connection_pool.connection
        end
        def self.connection?; !!connection; end

        def self.with_connection(key=nil, read_write=nil, &block)
          self.connection_pool.with_connection(key, read_write, &block)
        end

        def with_connection(key=nil, read_write=nil, &block)
          self.class.with_connection(key, read_write, &block)
        end

        def self.disconnect!
          self.async_connection_pool.disconnect! if @@async_connection_pool
          self.sync_connection_pool.disconnect! if @@sync_connection_pool
          @@sync_connection_pool = nil
          @@async_connection_pool = nil
        end

        def disconnect!
          self.class.disconnect!
        end

        def connection
          defined?(@connection) ? @connection : singleton_class.connection
        end

        def connection?
          !!connection
        end
      end
    end

    module ClassMethods
      DEFAULT_OPTIONS = {
        servers: "127.0.0.1:9160",
      }
      DEFAULT_THRIFT_OPTIONS = {
        exception_class_overrides: [],
      }

      # This doesn't open a connection. It merely conifgures the connection object.
      def establish_connection(config)
        spec = config.reverse_merge(DEFAULT_OPTIONS)

        spec[:thrift] = (spec[:thrift] || {}).reverse_merge(DEFAULT_THRIFT_OPTIONS)
        spec[:thrift][:exception_class_overrides] = spec[:thrift][:exception_class_overrides].map(&:constantize)

        self.connection_spec = spec
      end
    end
  end
end
