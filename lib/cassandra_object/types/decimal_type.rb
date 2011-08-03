module CassandraObject
  module Types
    module DecimalType
      def encode(value)
        raise ArgumentError.new("#{self} requires a BigDecimal") unless value.kind_of?(BigDecimal)
        value.to_s
      end
      module_function :encode

      def decode(str)
        return nil if str.empty?
        raise ArgumentError.new("Cannot convert #{str} into a BigDecimal") unless str.kind_of?(String)
        BigDecimal.new(str)
      end
      module_function :decode
    end
  end
end
