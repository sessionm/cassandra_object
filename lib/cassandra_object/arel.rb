# adds arel_table support for belongs_to relationships in ActiveRecord models
module CassandraObject
  module Arel
    extend ActiveSupport::Concern

    class_methods do
      def arel_table
        @arel_table ||= ::Arel::Table.new(column_family, arel_engine)
      end

      def arel_engine
        self
      end
    end
  end
end
