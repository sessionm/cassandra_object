module CassandraObject
  module Persistence
    extend ActiveSupport::Concern

    module ClassMethods
      def add(key, value, *columns_and_options)
        column_family, column, sub_column, options = CassandraObject::Base.with_connection(key, :write) { connection.extract_and_validate_params(self.column_family, key, columns_and_options, {}) }
        # the options are removed, leaving just columns
        columns = columns_and_options

        CassandraObject::Base.with_connection(key, :write) do
          ActiveSupport::Notifications.instrument("add.cassandra_object", column_family: column_family, key: key, column: column, sub_column: sub_column, value: value) do
            connection.add(column_family, key, value, *columns, :consistency => thrift_write_consistency)
          end
        end
      end

      def add_multiple_columns(key, hash, options = {})
        columns = hash.keys
        values = []
        if ! columns.empty? && hash[columns.first].is_a?(Hash)
          column_spec = hash.map { |column, sub_columns| sub_columns.keys.map { |sub_column| [column, sub_column].join('.') } }.flatten.join(', ')
          values = hash.map { |column, sub_columns| sub_columns.values }.flatten
        else
          column_spec = columns.join(', ')
          values = hash.values
        end
        values.uniq!
        value_spec = values.length == 1 ? values[0] : '<various>'

        CassandraObject::Base.with_connection(key, :write) do
          ActiveSupport::Notifications.instrument("add.cassandra_object", column_family: column_family, key: key, column: column_spec, value: value_spec) do
            connection.add_multiple_columns(column_family, key, hash, :consistency => thrift_write_consistency)
          end
        end
      end

      def remove(key)
        CassandraObject::Base.with_connection(key, :write) do
          ActiveSupport::Notifications.instrument("remove.cassandra_object", column_family: column_family, key: key) do
            connection.remove(column_family, key.to_s, consistency: thrift_write_consistency)
          end
        end
      end

      # remove_counter is not exposed by Cassandra gem.
      # TODO: move this to Cassandra gem.
      def remove_counter(key)
        CassandraObject::Base.with_connection(key, :write) do
          ActiveSupport::Notifications.instrument("remove.cassandra_object", column_family: column_family, key: key) do
            parent = CassandraThrift::ColumnParent.new(:column_family => column_family)
            connection.send(:client).remove_counter(key, parent, thrift_write_consistency)
          end
        end
      end

      def delete_all
        CassandraObject::Base.with_connection(nil, :write) do
          ActiveSupport::Notifications.instrument("truncate.cassandra_object", column_family: column_family) do
            connection.truncate!(column_family)
          end
        end
      end

      def create(attributes = {})
        new(attributes).tap do |object|
          object.save
        end
      end

      def write(key, attributes, schema_version)
        key.tap do |key|
          unless attributes.blank?
            attributes = encode_columns_hash(attributes, schema_version)
            CassandraObject::Base.with_connection(key, :write) do
              ActiveSupport::Notifications.instrument("insert.cassandra_object", column_family: column_family, key: key, attributes: attributes) do
              
                options = {}.tap do |options|
                  options[:consistency] = thrift_write_consistency
                  options[:ttl] = row_ttl unless row_ttl.nil?
                end

                connection.insert(column_family, key.to_s, attributes, options)
              end
            end
          end
        end
      end

      def instantiate(key, attributes)
        # remove any attributes we don't know about. we would do this earlier, but we want to make such
        #  attributes available to migrations
        attributes.delete_if { |k,_| model_attributes[k].nil? }

        allocate.tap do |object|
          object.instance_variable_set("@schema_version", attributes.delete('schema_version'))
          object.instance_variable_set("@key", parse_key(key))
          object.instance_variable_set("@new_record", false)
          object.instance_variable_set("@destroyed", false)
          object.instance_variable_set("@attributes", decode_columns_hash(attributes))
        end
      end

      def encode_columns_hash(attributes, schema_version)
        attributes.inject({}) do |memo, (column_name, value)|
          # cassandra stores bytes, not strings, so it has no concept of encodings. The ruby thrift gem 
          # expects all strings to be encoded as ascii-8bit.
          # don't attempt to encode columns that are nil
          memo[column_name.to_s] = value.nil? ? '' : model_attributes[column_name].converter.encode(value).force_encoding('ASCII-8BIT')
          memo
        end.merge({"schema_version" => schema_version.to_s})
      end

      def decode_columns_hash(attributes)
        Hash[attributes.map do |k, v|
               attribute = model_attributes[k]
               decoded = if attribute.converter.method(:decode).arity == 1
                           attribute.converter.decode(v)
                         else
                           attribute.converter.decode(v, attribute.options)
                         end
               [k.to_s, decoded]
             end]
      end
      
      def column_family_configuration
        [{:Name => column_family, :CompareWith => "UTF8Type"}]
      end
    end

    def new_record?
      @new_record
    end

    def destroyed?
      @destroyed
    end

    def persisted?
      !(new_record? || destroyed?)
    end

    def save(*)
      begin
        create_or_update
      rescue CassandraObject::RecordInvalid
        false
      end
    end

    def save!
      create_or_update || raise(RecordNotSaved)
    end

    def destroy
      self.class.remove(key)
      @destroyed = true
      freeze
    end

    def update_attribute(name, value)
      name = name.to_s
      send("#{name}=", value)
      save(:validate => false)
    end

    def update_attributes(attributes)
      self.attributes = attributes
      save
    end

    def update_attributes!(attributes)
      self.attributes = attributes
      save!
    end

    def reload(options = nil)
      clear_association_cache
      @attributes.update(self.class.find(self.id).instance_variable_get('@attributes'))
      self
    end

    private
      def create_or_update
        raise ReadOnlyRecord if readonly?
        result = new_record? ? create : update
        result != false
      end

      def create
        @key ||= self.class.next_key(self)
        write
        @new_record = false
        @key
      end
    
      def update
        write
      end

      def write
        changed_attributes = changed.inject({}) { |h, n| h[n] = read_attribute(n); h }
        self.class.write(key, changed_attributes, schema_version)
      end
  end
end
