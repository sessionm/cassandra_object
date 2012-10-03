module CassandraObject
  module Types
    module ArrayType
      def encode(array, opts={})
        raise ArgumentError.new("#{self} requires an Array") unless array.kind_of?(Array)
        array.to_json
      end
      module_function :encode

      def decode(str, opts={})
        ActiveSupport::JSON.decode(str)
      end
      module_function :decode
    end
  end
end
