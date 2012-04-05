module CassandraObject
  module Dirty
    extend ActiveSupport::Concern
    include ActiveModel::Dirty

    # Attempts to +save+ the record and clears changed attributes if successful.
    def save(*) #:nodoc:
      if status = super
        @previously_changed = changes
        @changed_attributes.clear
        @original_attribute_values = nil
      end
      status
    end

    # Attempts to <tt>save!</tt> the record and clears changed attributes if successful.
    def save!(*) #:nodoc:
      super.tap do
        @previously_changed = changes
        @changed_attributes.clear
        @original_attribute_values = nil
      end
    end

    def write_attribute(name, value)
      name = name.to_s

      # handle orig -> new -> orig
      if attribute_changed?(name)
        orig = changed_attributes[name]
        changed_attributes.delete(name) if orig == value
      else
        old = read_attribute(name)
        changed_attributes[name] = old if old != value
      end
      super
    end
  end
end
