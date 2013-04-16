module CassandraObject
  module Timestamps
    extend ActiveSupport::Concern

    included do
      # Derived classes must declare the :created_at and :updated_at attributes.

      before_create do #|r|
        if attribute_method?(:created_at)
          self.created_at ||= Time.current
        end
        if attribute_method?(:updated_at)
          self.updated_at ||= Time.current
        end
      end

      before_update if: :changed? do #|r|
        if attribute_method?(:updated_at)
          self.updated_at = Time.current
        end
      end
    end
  end
end
