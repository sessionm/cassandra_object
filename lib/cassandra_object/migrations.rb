module CassandraObject
  module Migrations
    extend ActiveSupport::Concern
    extend ActiveSupport::Autoload

    included do
      class_attribute :migrations
      self.migrations = []
      class_attribute :current_schema_version
      self.current_schema_version = 0
    end

    autoload :Migration
    
    class MigrationNotFoundError < StandardError
      def initialize(record_version, migrations)
        super("Cannot migrate a record from #{record_version.inspect}.  Migrations exist for #{migrations.map(&:version)}")
      end
    end
    
    module InstanceMethods
      def schema_version
        Integer(@schema_version || self.class.current_schema_version)
      end

      def attribute_will_change!(attribute)
        Rails.logger.warn "#{self.class}##{attribute} added/removed/changed and attribute_will_change! not implemented."
      end
    end
    
    module ClassMethods
      def migrate(version, &blk)
        self.migrations = self.migrations.dup
        self.migrations << Migration.new(version, blk)
        
        if version > self.current_schema_version 
          self.current_schema_version = version
        end
      end
      
      def instantiate(key, attributes)
        version = attributes.delete('schema_version')
        original_attributes = attributes.dup
        if version.to_s == current_schema_version.to_s
          return super(key, attributes)
        end
        
        versions_to_migrate = ((version.to_i + 1)..current_schema_version)
        
        migrations_to_run = versions_to_migrate.map do |v|
          migrations.find {|m| m.version == v}
        end
        
        if migrations_to_run.any?(&:nil?)
          raise MigrationNotFoundError.new(version, migrations)
        end
        
        migrations_to_run.inject(attributes) do |attrs, migration|
          migration.run(attrs)
          @schema_version = migration.version.to_s
          attrs
        end
        
        super(key, attributes).tap do |record|
          attributes.each do |name, _|
            record.attribute_will_change!(name) unless original_attributes.has_key?(name)
          end
        end
      end
    end
  end
end
