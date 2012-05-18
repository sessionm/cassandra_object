begin
  require 'em-synchrony'

  module CassandraObject
    module AsyncConnection
      extend ActiveSupport::Concern
      
      included do
        class_attribute :connection_spec

        class_eval do
          def self.connection()
            @@connection ||=
              begin
                spec = connection_spec.dup
                
                if EM.reactor_running?
                  require 'thrift_client/event_machine'
                  spec[:thrift].merge!(:transport => Thrift::EventMachineTransport,
                                       :transport_wrapper => nil)
                end

                EventMachine::Synchrony::ConnectionPool.new(:size => spec[:pool_size] || 5) do
                  Cassandra.new(spec[:keyspace], spec[:servers], spec[:thrift].dup)
                end.tap do |pool|
                  class << pool
                    def method_missing(method, *args, &blk)
                      execute(false) do |conn|
                        conn.send(method, *args, &blk)
                      end
                    end
                  end
                end
              end
          end
          def self.connection?() !!connection end

          def self.disconnect!; end

          def connection
            singleton_class.connection
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
rescue LoadError
  puts "Could not define alternate em-synchrony cassandra connection" if defined?($TESTING) && $TESTING
end
