module CassandraObject
  module AsyncConnection
    extend ActiveSupport::Concern
    
    included do
      class_attribute :connection_spec

      class_eval do
        @@fiber_connections = {}
        @@schema = nil
        def self.connection()
          @@fiber_connections[Fiber.current.object_id] ||=
            begin
              spec = connection_spec.dup
              
              if const_defined?(:EM) && EM.reactor_running?
                require 'thrift_client/event_machine'
                spec[:thrift].merge!(:transport => Thrift::EventMachineTransport,
                                     :transport_wrapper => nil)
              end
              
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
        end
        def self.connection?() !!connection end

        def self.disconnect!
          @@fiber_connections.delete(Fiber.current.object_id).tap { |conn|
            conn.disconnect! if conn
          }
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
