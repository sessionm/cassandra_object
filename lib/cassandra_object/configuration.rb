module CassandraObject
  module Configuration
    extend ActiveSupport::Concern
    
    module ClassMethods

      def recursive_symbolize_keys!(hash)
        hash.symbolize_keys!
        hash.values.select{|v| v.is_a? Hash}.each{|h| recursive_symbolize_keys!(h)}
      end

      @@config = nil
      def config=(config)
        raise('attempt to set config multiple times') if @@config

        recursive_symbolize_keys!(config)

        (@@config = config).tap do
          set_default_consistencies(@@config)
          establish_connection(@@config)
        end
      end
      def config
        @@config
      end

    end
  end
end
