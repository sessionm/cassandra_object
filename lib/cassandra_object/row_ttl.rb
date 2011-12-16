module CassandraObject
  module RowTTL
    extend ActiveSupport::Concern

    included do
      class_attribute :row_ttl
    end
  end
end
