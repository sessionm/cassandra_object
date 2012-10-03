if defined?(JRUBY_VERSION)
  require 'java'
  require 'hector'
end

module CassandraObject
  module Connection
    extend ActiveSupport::Concern
    
    included do
      class_attribute :_connection
      class_attribute :connection_spec
      class_attribute :connection_api
      self.connection_api = defined?(JRUBY_VERSION) ? :hector : :cassandra
    end

    module ClassMethods
      DEFAULT_OPTIONS = {
        servers: "127.0.0.1:9160",
        thrift: {}
      }
      DEFAULT_THRIFT_OPTIONS = {
        retries: 2,
        exception_classes: []
      }
      
      def connection
        self._connection ||= establish_connection(self.connection_spec)
      end

      def connection_string
        if :cassandra == self.connection_api
          self.connection.send(:client).current_server.connection_string
        else
          '?'
        end
      end

      def connection_sopts
        {:n_serializer => :string, :v_serializer => :string, :s_serializer => :string}
      end

      def establish_connection(spec)
        spec.reverse_merge!(DEFAULT_OPTIONS)
        spec[:thrift].reverse_merge!(DEFAULT_THRIFT_OPTIONS)

        case self.connection_api
        when :cassandra
          Cassandra.new(spec[:keyspace], spec[:servers], spec[:thrift].symbolize_keys!)
        when :hector
          cluster = Hector.cluster("Hector",
                                   spec[:servers].shuffle.first,
                                   :retries => spec[:thrift][:retries],
                                   :timeout => spec[:thrift][:timeout])
          
          Hector.new(spec[:keyspace], cluster)
        else
          raise "unexpected connection_api:#{self.connection_api}"
        end
      end
    end
  end
end
