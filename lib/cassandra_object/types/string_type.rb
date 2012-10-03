module CassandraObject
  module Types
    module StringType
      def encode(str, opts={})
        raise ArgumentError.new("#{self} requires a String") unless str.kind_of?(String)
        str.dup
      end
      module_function :encode

      def decode(str, opts={})
        str
      end
      module_function :decode
    end
  end
end
