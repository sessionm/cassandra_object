module CassandraObject
  module Associations
    extend ActiveSupport::Concern
    extend ActiveSupport::Autoload

    # TODO: is this the convention?
    include ActiveRecord::Reflection
    include ActiveRecord::Associations
    include ActiveRecord::AutosaveAssociation

    autoload :OneToMany
    autoload :OneToOne
    
    included do
      class_attribute :associations
      self.associations = {}
      class_attribute :pluralize_table_names, :instance_writer => false
    end

    module ClassMethods
      def relationships_column_family=(column_family)
        @relationships_column_family = column_family
      end

      def relationships_column_family
        @relationships_column_family || "#{name}Relationships"
      end

      def column_family_configuration
        super << {:Name=>relationships_column_family, :CompareWith=>"UTF8Type", :CompareSubcolumnsWith=>"TimeUUIDType", :ColumnType=>"Super"}
      end

      def dangerous_attribute_method?(name)
        false
      end

      def generated_association_methods
        @generated_association_methods ||= begin
          mod = const_set(:GeneratedAssociationMethods, Module.new)
          include mod
          mod
        end
      end

      def association(association_name, options= {})
        self.association = self.association.dup
        if options[:unique]
          self.associations[association_name] = OneToOne.new(association_name, self, options)
        else
          self.associations[association_name] = OneToMany.new(association_name, self, options)
        end
      end
      
      def remove(key)
        if connection.has_table?(relationships_column_family)
          begin
            CassandraObject::Base.with_connection(key, :write) do
              ActiveSupport::Notifications.instrument("remove.cassandra_object", column_family: relationships_column_family, key: key) do
                connection.remove(relationships_column_family, key.to_s, consistency: thrift_write_consistency)
              end
            end
          rescue Cassandra::Errors::InvalidError => e
            # pretty sure this is not the correct message for cassandra-driver gem, will need to investigate the actual message
            raise e unless e.message =~ /unconfigured columnfamily/i
          end
        end

        super
      end
    end
  end
end
