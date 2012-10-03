module CassandraObject
  class Attribute
    attr_reader :name, :converter, :expected_type, :options
    def initialize(name, converter, expected_type, options)
      @name          = name.to_s
      @converter     = converter
      @expected_type = expected_type
      @options       = options
    end

    def check_value!(value)
      return value if value.nil?
      if value.kind_of?(expected_type)
        value
      elsif converter.method(:decode).arity == 1
        converter.decode(value)
      else
        converter.decode(value, @options)
      end
    end
  end

  module Attributes
    extend ActiveSupport::Concern
    include ActiveModel::AttributeMethods

    module ClassMethods
      def attribute(name, options)
        if model_attributes.empty?
          self.model_attributes = {}.with_indifferent_access
        elsif (model_attributes.size == 2) && model_attributes.include?(:created_at) && model_attributes.include?(:updated_at)
          self.model_attributes = self.model_attributes.dup
        end

        if type_mapping = CassandraObject::Type.get_mapping(options[:type])
          converter = type_mapping.converter
          expected_type = type_mapping.expected_type
        elsif options[:converter]
          converter = options[:converter]
          expected_type = options[:type]
        else
          raise "Unknown type #{options[:type]}"
        end
        
        model_attributes[name] = Attribute.new(name, converter, expected_type, options.except(:type, :converter))
      end

      def define_attribute_methods
        unless defined?(@attribute_methods_mutex)
          @attribute_methods_mutex = Mutex.new
        end

        # Use a mutex; we don't want two thread simaltaneously trying to define
        # attribute methods.
        @attribute_methods_mutex.synchronize do
          return if attribute_methods_generated?
          superclass.define_attribute_methods unless self == base_class
          super(model_attributes.keys)
          @attribute_methods_generated = true
        end
      end

      def attribute_methods_generated?
        @attribute_methods_generated ||= false
      end

      def undefine_attribute_methods
        super
        @attribute_methods_generated = false
      end
    end

    included do
      class_attribute :model_attributes
      self.model_attributes = {}.with_indifferent_access

      attribute_method_suffix("", "=")      
    end

    def write_attribute(name, value)
      if ma = self.class.model_attributes[name]
        @attributes[name.to_s] = ma.check_value!(value)
      else
        raise NoMethodError, "Unknown attribute #{name.inspect}"
      end
    end

    def read_attribute(name)
      @attributes[name.to_s]
    end

    def attributes=(attributes)
      attributes.each do |(name, value)|
        send("#{name}=", value)
      end
    end

    def method_missing(method_id, *args, &block)
      if !self.class.attribute_methods_generated?
        self.class.define_attribute_methods
        send(method_id, *args, &block)
      else
        super
      end
    end

    def respond_to?(*args)
      self.class.define_attribute_methods unless self.class.attribute_methods_generated?
      super
    end

    protected
      def attribute_method?(name)
        !!model_attributes[name.to_sym]
      end

    private
      def attribute(name)
        read_attribute(name)
      end
    
      def attribute=(name, value)
        write_attribute(name, value)
      end
  end
end
