module CassandraObject
  module FinderMethods
    extend ActiveSupport::Concern
    module ClassMethods
      def find(key, opts={})
        # kludge to play nice ActiveRecord association
        opts.assert_valid_keys(:conditions, :consistency)
        opts[:consistency] ||= thrift_read_consistency
        raise(ArgumentError, "unexpected conditions") if opts[:conditions].present?
        raise(CassandraObject::InvalidKey, "invalid key: #{key}") if key.blank? || ! parse_key(key)

        attributes =
          begin
            CassandraObject::Base.with_connection(key, :read) do
              ActiveSupport::Notifications.instrument("get.cassandra_object", column_family: column_family, key: key) do
                connection.get column_family, key, opts.slice(:consistency)
              end
            end
          end

        if attributes && ! attributes.empty?
          instantiate(key, attributes)
        else
          raise CassandraObject::RecordNotFound
        end
      end

      def find_by_id(key, opts={})
        find(key, opts)
      rescue CassandraObject::RecordNotFound
        nil
      end

      def get_counter(key, column, opts={})
        opts.assert_valid_keys(:consistency)
        opts[:consistency] ||= thrift_read_consistency

        result = CassandraObject::Base.with_connection(key, :read) do
          ActiveSupport::Notifications.instrument("get_counter.cassandra_object", column_family: column_family, key: key, column: column) do
            connection.get(column_family, key, column, opts)
          end
        end

        if result
          result
        else
          raise CassandraObject::RecordNotFound
        end
      end

      def all(options={})
        CassandraObject::Relation.new(self, self.arel_table, options)
      end

      def first(options=nil)
        result = CassandraObject::Base.with_connection(nil, :read) do
          ActiveSupport::Notifications.instrument("get_range.cassandra_object", column_family: column_family, key_count: 1) do
            connection.get_range(column_family, key_count: 1, consistency: thrift_read_consistency)
          end
        end.first

        result ? instantiate(result[0], result[1]) : nil
      end

      def find_by_sql(arel, bind_values)
        if bind_values.size == 0
          limit = 100
          results = CassandraObject::Base.with_connection(nil, :read) do
            ActiveSupport::Notifications.instrument("get_range.cassandra_object", column_family: column_family, key_count: limit) do
              connection.get_range(column_family, key_count: limit, consistency: thrift_read_consistency)
            end
          end

          return results.map do |k, v|
            v.empty? ? nil : instantiate(k, v)
          end.compact
        elsif bind_values.size == 1
          if bind_values[0][0] == 'id' && bind_values[0][1].is_a?(String)
            return [find_by_id(bind_values[0][1])]
          elsif bind_values[0][:id]
            return [find_by_id(bind_values[0][:id])]
          end
        end

        raise "only supports lookups by id currently bind_values #{bind_values}"
      end

      def unscoped
        CassandraObject::Relation.new(self, self.arel_table, {})
      end
      alias current_scope unscoped

      def find_with_ids(*ids)
        expects_array = ids.first.kind_of?(Array)
        return ids.first if expects_array && ids.first.empty?

        ids = ids.dup
        ids.flatten!
        ids.compact!
        ids.collect!(&:to_s)
        ids.uniq!

        #raise RecordNotFound, "Couldn't find #{record_klass.name} without an ID" if ids.empty?

        results = multi_get(ids).values.compact

        results.size <= 1 && !expects_array ? results.first : results
      end

      def find_all_by_expression(expression, options={})
        multi_get_by_expression(expression, options).values
      end

      # Selecting a slice of a super column
      def get_slice(key, start, finish, opts={})
        CassandraObject::Base.with_connection(key, :read) do
          connection.get_slice(column_family,
                               key, 
                               start,
                               finish,
                               opts[:count] || 100,
                               opts[:reversed] || false,
                               opts[:consistency] || thrift_read_consistency)
        end
      end

      private
        def _columns_to_hash(columns)
          {}.tap do |hsh|
            columns.each { |column| hsh[column.name] = column.value }
          end
        end

        def instantiate_many(attribute_results)
          attribute_results.inject({}) do |memo, (key, attributes)|
            if attributes.empty?
              memo[key] = nil
            else
              memo[parse_key(key)] = instantiate(key, attributes)
            end
            memo
          end
        end

        def multi_get(keys, options={})
          attribute_results = CassandraObject::Base.with_connection(nil, :read) do
            ActiveSupport::Notifications.instrument("multi_get.cassandra_object", column_family: column_family, keys: keys) do
              connection.multi_get(column_family, keys.map(&:to_s), consistency: thrift_read_consistency)
            end
          end

          instantiate_many(attribute_results)
        end

        def multi_get_by_expression(expression, options={})
          options = options.reverse_merge(:consistency => thrift_read_consistency)

          attribute_results = CassandraObject::Base.with_connection(nil, :read) do
            ActiveSupport::Notifications.instrument("multi_get_by_expression.cassandra_object", column_family: column_family, expression: expression) do
              intermediate_results = connection.get_indexed_slices(column_family, expression, options)
              connection.send(:multi_columns_to_hash!, column_family, intermediate_results)
            end
          end

          instantiate_many(attribute_results)
        end

        def get(key, options={})
          multi_get([key], options).values.first
        end
    end
  end
end
