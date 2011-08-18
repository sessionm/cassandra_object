module CassandraObject
  class Base
    class ConnectionSpecification #:nodoc:
      attr_reader :config, :adapter_method
      def initialize (config, adapter_method)
        @config, @adapter_method = config, adapter_method
      end
    end

    ##
    # :singleton-method:
    # The connection handler
    class_attribute :connection_handler, :instance_writer => false
    self.connection_handler = ConnectionAdapters::ConnectionHandler.new

    # Returns the connection currently associated with the class. This can
    # also be used to "borrow" the connection to do database work that isn't
    # easily done without going straight to SQL.
    def connection
      self.class.connection
    end

    def self.establish_connection(spec = nil)
      self.connection_handler.establish_connection(self.name, spec)
    end

    class << self
      # Returns the connection currently associated with the class. This can
      # also be used to "borrow" the connection to do database work unrelated
      # to any of the specific Active Records.
      def connection
        retrieve_connection
      end

      def connection_pool
        connection_handler.retrieve_connection_pool(self)
      end

      def retrieve_connection
        connection_handler.retrieve_connection(self)
      end

      # Returns true if Active Record is connected.
      def connected?
        connection_handler.connected?(self)
      end

      def remove_connection(klass = self)
        connection_handler.remove_connection(klass)
      end

      delegate :clear_active_connections!, :clear_reloadable_connections!,
        :clear_all_connections!,:verify_active_connections!, :to => :connection_handler
    end
  end
end
