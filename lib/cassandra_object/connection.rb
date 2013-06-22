require 'with_connection'

module CassandraObject
  module Connection
    extend ActiveSupport::Concern
    
    included do
      class_attribute :connection_spec

      class_eval do
        def self.new_event_machine_connection(servers=nil)
          spec = connection_spec.dup
              
          require 'thrift_client/event_machine'
          spec[:thrift].merge!(:transport => Thrift::EventMachineTransport,
                               :transport_wrapper => nil)
              
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

        def new_event_machine_connection(servers=nil)
          self.class.new_event_machine_connection(servers)
        end

        def self.new_connection(servers=nil)
          spec = connection_spec.dup
              
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

        def new_connection(servers=nil)
          self.class.new_connection(servers)
        end

        @@schema = nil
        @@async_connection_pool = nil
        @@sync_connection_pool = nil
        def self.async_connection_pool
          @@async_connection_pool ||=
            begin
              adapter_method = Proc.new do
                self.new_event_machine_connection
              end
              spec = ActiveRecord::Base::ConnectionSpecification.new self.connection_spec, adapter_method
              WithConnection::ConnectionPool.new "async cassandra", spec
            end
        end
        def async_connection_pool
          self.class.async_connection_pool
        end

        def self.sync_connection_pool
          @@sync_connection_pool ||=
            begin
              adapter_method = Proc.new do
                self.new_connection
              end
              spec = ActiveRecord::Base::ConnectionSpecification.new self.connection_spec, adapter_method
              WithConnection::ConnectionPool.new "sync cassandra", spec
            end
        end
        def sync_connection_pool
          self.class.sync_connection_pool
        end

        def self.ring
          self.new_connection.ring
        end

        def self.servers_and_ranges(datacenter)
          datacenter = datacenter.to_s
          ring.map do |t| 
            {
              :start_token => sprintf('%032x', t.start_token.to_i), 
              :end_token => t.end_token.to_i == 0 ? ('f' * 32) : sprintf('%032x', t.end_token.to_i - 1), 
              :servers => t.endpoint_details.select { |d| d.datacenter == datacenter }.map(&:host)
            }
          end
        end

        @@ranged_connection_pool_key_algo = nil
        def self.ranged_connection_pool_key_algo
          @@ranged_connection_pool_key_algo ||= Proc.new { |key| Digest::MD5.hexdigest key }
        end

        def self.create_ranged_connection_pool(async)
          require 'with_connection/ranged_connection_pool'

          adapter_method = Proc.new do
            async ? self.new_event_machine_connection : self.new_connection
          end

          spec = ActiveRecord::Base::ConnectionSpecification.new self.connection_spec, adapter_method
          default_pool = WithConnection::ConnectionPool.new "sync cassandra", spec

          ranges_and_pools = self.servers_and_ranges(self.connection_spec[:datacenter]).map do |info|
            conn_method = Proc.new do
              async ? self.new_event_machine_connection(info[:servers]) : self.new_connection(info[:servers])
            end
            spec = ActiveRecord::Base::ConnectionSpecification.new self.connection_spec, conn_method
            pool = WithConnection::ConnectionPool.new "sync cassandra", spec
            [WithConnection::RangedConnectionPool::BasicRange.new(info[:start_token], info[:end_token]), pool]
          end

          ranges_and_pools.size <= 1 ? default_pool : WithConnection::RangedConnectionPool.new(ranges_and_pools, default_pool, self.ranged_connection_pool_key_algo)
        end

        @@sync_ranged_connection_pool = nil
        @@async_ranged_connection_pool = nil
        def self.sync_ranged_connection_pool
          @@sync_ranged_connection_pool ||= create_ranged_connection_pool(false)
        end

        def self.async_ranged_connection_pool
          @@async_ranged_connection_pool ||= create_ranged_connection_pool(true)
        end

        if defined?(EM)
          def self.ranged_connection_pool
            EM.reactor_running? ? self.async_ranged_connection_pool : self.sync_ranged_connection_pool
          end
        else
          def self.ranged_connection_pool
            self.sync_ranged_connection_pool
          end
        end

        def ranged_connection_pool
          self.class.ranged_connection_pool
        end

        if defined?(EM)
          def self.connection_pool
            if self.connection_spec[:datacenter]
              EM.reactor_running? ? self.async_ranged_connection_pool : self.sync_ranged_connection_pool
            else
              EM.reactor_running? ? self.async_connection_pool : self.sync_connection_pool
            end
          end
        else
          def self.connection_pool
            self.connection_spec[:datacenter] ? self.ranged_connection_pool : self.sync_connection_pool
          end
        end

        def connection_pool
          self.class.connection_pool
        end

        def self.connection()
          self.connection_pool.connection
        end
        def self.connection?() !!connection end

        def self.with_connection(key=nil, &block)
          self.connection_pool.with_connection(&block)
        end

        def with_connection(key=nil, &block)
          self.class.with_connection(key, &block)
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
