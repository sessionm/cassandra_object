require 'with_connection'

module CassandraObject
  module AsyncConnection
    extend ActiveSupport::Concern
    
    included do
      class_attribute :connection_spec

      class_eval do
        def self.new_event_machine_connection
          spec = connection_spec.dup
              
          require 'thrift_client/event_machine'
          spec[:thrift].merge!(:transport => Thrift::EventMachineTransport,
                               :transport_wrapper => nil)
              
          Cassandra.new(spec[:keyspace], spec[:servers], spec[:thrift]).tap do |conn|
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

        def new_event_machine_connection
          self.class.new_event_machine_connection
        end

        def self.new_connection
          spec = connection_spec.dup
              
          Cassandra.new(spec[:keyspace], spec[:servers], spec[:thrift]).tap do |conn|
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

        def new_connection
          self.class.new_connection
        end

        @@schema = nil
        @@connection_pool = nil
        def self.connection_pool
          @@connection_pool ||=
            begin
              adapter_method = Proc.new do
                EM.reactor_running? ? self.new_event_machine_connection : self.new_connection
              end
              spec = ActiveRecord::Base::ConnectionSpecification.new self.connection_spec, adapter_method
              WithConnection::ConnectionPool.new "cassandra", spec
            end
        end
        def connection_pool
          self.class.connection_pool
        end

        def self.connection()
          self.connection_pool.connection
        end
        def self.connection?() !!connection end

        def self.with_connection(&block)
          self.connection_pool.with_connection(&block)
        end

        def with_connection(&block)
          self.class.with_connection(&block)
        end

        def self.disconnect!
          self.connection_pool.disconnect!
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
