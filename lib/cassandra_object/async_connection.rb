module CassandraObject
  module AsyncConnection
    extend ActiveSupport::Concern
    
    included do
      class_attribute :connection_spec

      class_eval do
        @@fiber_connections = {}
        def self.connection()
          @@fiber_connections[Fiber.current.object_id] ||=
            begin
              spec = connection_spec.dup
              
              if EM.reactor_running?
                require 'thrift_client/event_machine'
                spec[:thrift].merge!(:transport => Thrift::EventMachineTransport,
                                     :transport_wrapper => nil)
              end
              
              Cassandra.new(spec[:keyspace], spec[:servers], spec[:thrift])
            end
        end
        def self.connection?() !!connection end

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
        thrift: {}
      }
      def establish_connection(spec)
        spec.reverse_merge!(DEFAULT_OPTIONS)
        spec[:thrift].symbolize_keys!
        self.connection_spec = spec
      end
    end
  end
end
