require 'active_support/core_ext/hash/except'
require 'active_support/core_ext/object/try'
require 'active_support/core_ext/object/blank'
require 'active_support/core_ext/hash/indifferent_access'

module CassandraObject
  module NestedAttributes #:nodoc:
    class TooManyRecords < CassandraObjectError
    end

    extend ActiveSupport::Concern

    included do
      class_attribute :nested_attributes_options
      self.nested_attributes_options = {}
    end

    module ClassMethods
      REJECT_ALL_BLANK_PROC = proc { |attributes| attributes.all? { |_, value| value.blank? } }

      def accepts_nested_attributes_for(*attr_names)
        options = { :allow_destroy => false, :update_only => false }
        options.update(attr_names.extract_options!)
        options.assert_valid_keys(:allow_destroy, :reject_if, :limit, :update_only)
        options[:reject_if] = REJECT_ALL_BLANK_PROC if options[:reject_if] == :all_blank

        attr_names.each do |association_name|
          if reflection = reflect_on_association(association_name)
            reflection.options[:autosave] = true
            add_autosave_association_callbacks(reflection)
            self.nested_attributes_options = self.nested_attributes_options.dup
            nested_attributes_options[association_name.to_sym] = options
            type = (reflection.collection? ? :collection : :one_to_one)

            # def pirate_attributes=(attributes)
            #   assign_nested_attributes_for_one_to_one_association(:pirate, attributes)
            # end
            class_eval <<-eoruby, __FILE__, __LINE__ + 1
              if method_defined?(:#{association_name}_attributes=)
                remove_method(:#{association_name}_attributes=)
              end
              def #{association_name}_attributes=(attributes)
                assign_nested_attributes_for_#{type}_association(:#{association_name}, attributes)
              end
            eoruby
          else
            raise ArgumentError, "No association found for name `#{association_name}'. Has it been defined yet?"
          end
        end
      end
    end

    def _destroy
      marked_for_destruction?
    end

    private

    UNASSIGNABLE_KEYS = %w( id _destroy )

    def assign_nested_attributes_for_one_to_one_association(association_name, attributes)
      options = nested_attributes_options[association_name]
      attributes = attributes.with_indifferent_access
      check_existing_record = (options[:update_only] || !attributes['id'].blank?)

      if check_existing_record && (record = send(association_name)) &&
          (options[:update_only] || record.id.to_s == attributes['id'].to_s)
        assign_to_or_mark_for_destruction(record, attributes, options[:allow_destroy]) unless call_reject_if(association_name, attributes)

      elsif !attributes['id'].blank?
        raise_nested_attributes_record_not_found(association_name, attributes['id'])

      elsif !reject_new_record?(association_name, attributes)
        method = "build_#{association_name}"
        if respond_to?(method)
          send(method, attributes.except(*UNASSIGNABLE_KEYS))
        else
          raise ArgumentError, "Cannot build association #{association_name}. Are you trying to build a polymorphic one-to-one association?"
        end
      end
    end

    def assign_nested_attributes_for_collection_association(association_name, attributes_collection)
      options = nested_attributes_options[association_name]

      unless attributes_collection.is_a?(Hash) || attributes_collection.is_a?(Array)
        raise ArgumentError, "Hash or Array expected, got #{attributes_collection.class.name} (#{attributes_collection.inspect})"
      end

      if options[:limit] && attributes_collection.size > options[:limit]
        raise TooManyRecords, "Maximum #{options[:limit]} records are allowed. Got #{attributes_collection.size} records instead."
      end

      if attributes_collection.is_a? Hash
        keys = attributes_collection.keys
        attributes_collection = if keys.include?('id') || keys.include?(:id)
          Array.wrap(attributes_collection)
        else
          attributes_collection.sort_by { |i, _| i.to_i }.map { |_, attributes| attributes }
        end
      end

      association = send(association_name)

      existing_records = if association.loaded?
        association.to_a
      else
        attribute_ids = attributes_collection.map {|a| a['id'] || a[:id] }.compact
        attribute_ids.present? ? association.all(:conditions => {association.primary_key => attribute_ids}) : []
      end

      attributes_collection.each do |attributes|
        attributes = attributes.with_indifferent_access

        if attributes['id'].blank?
          unless reject_new_record?(association_name, attributes)
            association.build(attributes.except(*UNASSIGNABLE_KEYS))
          end

        elsif existing_record = existing_records.detect { |record| record.id.to_s == attributes['id'].to_s }
          association.send(:add_record_to_target_with_callbacks, existing_record) if !association.loaded? && !call_reject_if(association_name, attributes)
          assign_to_or_mark_for_destruction(existing_record, attributes, options[:allow_destroy])

        else
          raise_nested_attributes_record_not_found(association_name, attributes['id'])
        end
      end
    end

    # Updates a record with the +attributes+ or marks it for destruction if
    # +allow_destroy+ is +true+ and has_destroy_flag? returns +true+.
    def assign_to_or_mark_for_destruction(record, attributes, allow_destroy)
      record.attributes = attributes.except(*UNASSIGNABLE_KEYS)
      record.mark_for_destruction if has_destroy_flag?(attributes) && allow_destroy
    end

    # Determines if a hash contains a truthy _destroy key.
    def has_destroy_flag?(hash)
      value = hash['_destroy']
      if value.is_a?(String) && value.blank?
        nil
      else
        [true, 1, '1', 't', 'T', 'true', 'TRUE'].include?(value)
      end
    end

    # Determines if a new record should be build by checking for
    # has_destroy_flag? or if a <tt>:reject_if</tt> proc exists for this
    # association and evaluates to +true+.
    def reject_new_record?(association_name, attributes)
      has_destroy_flag?(attributes) || call_reject_if(association_name, attributes)
    end

    def call_reject_if(association_name, attributes)
      return false if has_destroy_flag?(attributes)
      case callback = nested_attributes_options[association_name][:reject_if]
      when Symbol
        method(callback).arity == 0 ? send(callback) : send(callback, attributes)
      when Proc
        callback.call(attributes)
      end
    end

    def raise_nested_attributes_record_not_found(association_name, record_id)
      reflection = self.class.reflect_on_association(association_name)
      raise RecordNotFound, "Couldn't find #{reflection.klass.name} with ID=#{record_id} for #{self.class.name} with ID=#{id}"
    end
  end
end
