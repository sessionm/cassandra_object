require 'active_record/connection_adapters/connection_specification'

module CassandraObject
  module Connection
    extend ActiveSupport::Concern
    
    included do
      class_attribute :connection_spec

      def connection
        self.class.connection
      end

      def connection?
        self.class.connection?
      end

      def disconnect!
        self.class.disconnect!
      end

      def with_connection(*args)
        yield
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

      def connection
        @connection ||= CassandraObject::Adapters::CassandraDriver.new(self.connection_spec).client
      end

      def connection?
        !! @connection
      end

      def disconnect!
        @connection.try(:close)
        @connection = nil
      end

      def with_connection(*args)
        yield
      end
    end
  end
end
