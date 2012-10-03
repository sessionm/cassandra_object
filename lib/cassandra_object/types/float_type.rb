module CassandraObject
  module Types
    module FloatType
      REGEX = /\A[-+]?\d+(\.\d+)?\Z/
      def encode(float, opts={})
        raise ArgumentError.new("#{self} requires a Float") unless float.kind_of?(Float)
        float.to_s
      end
      module_function :encode

      def decode(str, opts={})
        return nil if str.empty?
        raise ArgumentError.new("Cannot convert #{str} into a Float") unless str.kind_of?(String) && str.match(REGEX)
        str.to_f
      end
      module_function :decode
    end
  end
end
