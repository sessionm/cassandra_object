module CassandraObject
  module Types
    module HashType
      def encode(hash, opts={})
        raise ArgumentError.new("#{self} requires a Hash") unless hash.kind_of?(Hash)
        ActiveSupport::JSON.encode(hash)
      end
      module_function :encode

      def decode(str, opts={})
        ActiveSupport::JSON.decode(str)
      end
      module_function :decode
    end
  end
end
