module CassandraObject
  module Mocking
    extend ActiveSupport::Concern
    module ClassMethods
      def use_mock!(really=true)
        self.connection_class = Cassandra
      end
    end
  end
end
