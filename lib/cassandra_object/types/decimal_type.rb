module CassandraObject
  module Types
    module DecimalType
      def encode(value)
        return nil if value.nil?
        raise ArgumentError.new("#{self} requires a Numeric") unless value.kind_of?(Numeric)
        value.to_s
      end
      module_function :encode

      def decode(value, options={})
        return nil if value.nil?
        value = value.to_s if value.kind_of?(Numeric)
        raise ArgumentError.new("Cannot convert #{value} into a BigDecimal") unless value.kind_of?(String)
        if options[:precision].present?
          BigDecimal.new(value, options[:precision])
        else
          BigDecimal.new(value)
        end
      end
      module_function :decode
    end
  end
end
