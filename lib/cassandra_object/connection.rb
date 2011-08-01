module CassandraObject
  module Connection
    extend ActiveSupport::Concern
    
    included do
      class_attribute :connection_spec
    end

    module ClassMethods
      DEFAULT_OPTIONS = {
        servers: "127.0.0.1:9160",
        thrift: {}
      }
      def establish_connection(spec)
        spec.reverse_merge!(DEFAULT_OPTIONS)
        @connection = Cassandra.new(spec[:keyspace], spec[:servers], spec[:thrift].symbolize_keys!)
      end

      def connection=(val)
        @connection = val
      end

      def connection
        establish_connection(connection_spec) if @connection.nil?
        @connection
      end
    end
  end
end
