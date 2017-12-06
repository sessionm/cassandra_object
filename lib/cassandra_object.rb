require 'active_support/all'
require 'active_record'

module CassandraObject
  extend ActiveSupport::Autoload

  autoload :Base
  autoload :Configuration
  autoload :Connection
  autoload :Attributes
  autoload :Dirty
  autoload :Consistency
  autoload :Persistence
  autoload :Callbacks
  autoload :Validations
  autoload :Identity
  autoload :Serialization
  autoload :Associations
  autoload :Migrations
  autoload :Cursor
  autoload :Collection
  autoload :Mocking
  autoload :Batches
  autoload :FinderMethods
  autoload :Timestamps
  autoload :Type
  autoload :Schema
  autoload :RowTTL
  autoload :NestedAttributes
  autoload :Adapters
  autoload :Arel
  autoload :Relation

  module Tasks
    extend ActiveSupport::Autoload
    autoload :Keyspace
    autoload :ColumnFamily
  end

  module Types
    extend ActiveSupport::Autoload
    
    autoload :ArrayType
    autoload :BooleanType
    autoload :DateType
    autoload :DecimalType
    autoload :FloatType
    autoload :HashType
    autoload :IntegerType
    autoload :SetType
    autoload :TimeType
    autoload :TimeWithZoneType
    autoload :UTF8StringType
  end
end

require 'cassandra_object/custom_comparable'
require 'cassandra_object/composite'
require 'cassandra_object/long'
require 'cassandra_object/railtie'
