module CassandraObject
  module Connection
    extend ActiveSupport::Concern
    
    included do
      class_attribute :connection
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

        spec[:thrift] = (spec[:thrift].try(:symbolize_keys) || {}).reverse_merge(DEFAULT_THRIFT_OPTIONS)
        spec[:thrift][:exception_class_overrides] = spec[:thrift][:exception_class_overrides].map(&:constantize)

        self.connection = Cassandra.new(spec[:keyspace], spec[:servers], spec[:thrift])
        self.connection.disable_node_auto_discovery! if spec[:disable_node_auto_discovery]
        self.connection
      end
    end
  end
end
