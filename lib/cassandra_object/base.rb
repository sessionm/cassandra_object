require 'set'

require 'cassandra_object/log_subscriber'
require 'cassandra_object/types'
require 'cassandra_object/errors'

module CassandraObject
  class Base
    class << self
      def column_family=(column_family)
        @column_family = column_family
      end

      def column_family
        @column_family || name.pluralize
      end

      def base_class
        klass = self
        while klass.superclass != Base
          klass = klass.superclass
        end
        klass
      end

      delegate :compute_type, :to => 'ActiveRecord::Base'
    end

    extend ActiveModel::Naming
    include ActiveModel::Conversion
    extend ActiveSupport::DescendantsTracker

    include Configuration
    include Connection
    include Consistency
    include RowTTL
    include Identity
    include Attributes
    include Persistence
    include Dirty
    include Validations
    include Callbacks
    include Associations
    include Batches
    include FinderMethods
    include Timestamps
    include NestedAttributes

    attr_accessor :key

    include Serialization
    include Migrations
    include Mocking

    def initialize(attributes={})
      @key = attributes.delete(:key)
      @new_record = true
      @destroyed = false
      @readonly = false
      @attributes = {}.with_indifferent_access
      self.attributes = attributes
      @schema_version = self.class.current_schema_version
    end

    # Returns +true+ if the record is read only.
    def readonly?
      @readonly
    end
    
    def readonly=(value)
      @readonly = value
    end

    # Marks this record as read only.
    def readonly!
      self.readonly = true
    end

    def attributes
      @attributes.merge(:id => id)
    end

    def to_param
      id.to_s if persisted?
    end

    def hash
      id.hash
    end    

    def ==(comparison_object)
      comparison_object.equal?(self) ||
        (comparison_object.instance_of?(self.class) &&
          comparison_object.key == key &&
          !comparison_object.new_record?)
    end

    def eql?(comparison_object)
      self == (comparison_object)
    end

    # Returns the value of the attribute identified by <tt>attr_name</tt> after it has been typecast (for example,
    # "2004-12-12" in a data column is cast to a date object, like Date.new(2004, 12, 12)).
    # (Alias for the protected read_attribute method).
    def [](attr_name)
      read_attribute(attr_name)
    end
    
    # Updates the attribute identified by <tt>attr_name</tt> with the specified +value+.
    # (Alias for the protected write_attribute method).
    def []=(attr_name, value)
      write_attribute(attr_name, value)
    end
  end
end
