module CassandraObject
  module FinderMethods
    extend ActiveSupport::Concern
    module ClassMethods
      def find(key, opts={})
        # kludge to play nice ActiveRecord association
        opts.assert_valid_keys(:conditions)
        raise(ArgumentError, "unexpected conditions") if opts[:conditions].present?
        raise(CassandraObject::InvalidKey, "invalid key: #{key}") if key.blank? || ! parse_key(key)

        if (attributes = ActiveSupport::Notifications.instrument("get.cassandra_object", column_family: column_family, key: key) { connection.get(column_family, key) }) &&
           !attributes.empty?
          instantiate(key, attributes)
        else
          raise CassandraObject::RecordNotFound
        end
      end

      def find_by_id(key)
        find(key)
      rescue CassandraObject::RecordNotFound
        nil
      end

      def get_counter(key, column)
        result = ActiveSupport::Notifications.instrument("get_counter.cassandra_object", column_family: column_family, key: key, column: column) do
          connection.get(column_family, key, column, consistency: thrift_read_consistency)
        end

        if result
          result
        else
          raise CassandraObject::RecordNotFound
        end
      end

      def all(options = {})
        limit = options[:limit] || 100
        results = ActiveSupport::Notifications.instrument("get_range.cassandra_object", column_family: column_family, key_count: limit) do
          connection.get_range(column_family, key_count: limit, consistency: thrift_read_consistency)
        end

        results.map do |k, v|
          v.empty? ? nil : instantiate(k, v)
        end.compact
      end

      def first(options = {})
        all(options.merge(:limit => 1)).first
      end

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

      # Selecting a slice of a super column is not supported by default with the cassandra gem
      # TODO: move this to Cassandra gem.
      def get_slice(key, start, finish, opts={})
        parent = CassandraThrift::ColumnParent.new(:column_family => column_family)
        predicate = CassandraThrift::SlicePredicate.
          new(:slice_range =>
              CassandraThrift::SliceRange.new(:start => start,
                                              :finish => finish,
                                              :count => opts[:count] || 100,
                                              :reversed => opts[:reversed] || false))
        
        ActiveSupport::Notifications.instrument("get_slice.cassandra_object", column_family: column_family, key: key, start: start, finish: finish) do
          {}.tap do |result|
            connection.send(:client).get_slice(key, parent, predicate, opts[:consistency] || Cassandra::Consistency::ONE).each do |column|
              result[column.counter_super_column.name] = _columns_to_hash(column.counter_super_column.columns)
            end
          end
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
          attribute_results = ActiveSupport::Notifications.instrument("multi_get.cassandra_object", column_family: column_family, keys: keys) do
            connection.multi_get(column_family, keys.map(&:to_s), consistency: thrift_read_consistency)
          end

          instantiate_many(attribute_results)
        end

        def multi_get_by_expression(expression, options={})
          options = options.reverse_merge(:consistency => thrift_read_consistency)

          attribute_results = ActiveSupport::Notifications.instrument("multi_get_by_expression.cassandra_object", column_family: column_family, expression: expression) do
            intermediate_results = connection.get_indexed_slices(column_family, expression, options)
            connection.send(:multi_columns_to_hash!, column_family, intermediate_results)
          end

          instantiate_many(attribute_results)
        end

        def get(key, options={})
          multi_get([key], options).values.first
        end
    end
  end
end
