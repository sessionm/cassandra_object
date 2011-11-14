module CassandraObject
  module FinderMethods
    extend ActiveSupport::Concern
    module ClassMethods
      def find(key)
        if key.present? &&
           parse_key(key) &&
           (attributes = ActiveSupport::Notifications.instrument("get.cassandra_object", column_family: column_family, key: key) { connection.get(column_family, key) }) &&
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
          connection.get(column_family, key, column)
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

      private
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
