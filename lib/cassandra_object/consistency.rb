if defined?(Hector)
  java_import 'me.prettyprint.hector.api.HConsistencyLevel'
end

module CassandraObject
  module Consistency
    extend ActiveSupport::Concern

    included do
      cattr_accessor :consistency_levels
      self.consistency_levels = [:one, :quorum, :all]

      class_attribute :write_consistency
      class_attribute :read_consistency
      self.write_consistency  = :quorum
      self.read_consistency   = :quorum
    end

    module ClassMethods
      if defined?(Cassandra)
        THRIFT_LEVELS = {
          :one    => Cassandra::Consistency::ONE,
          :quorum => Cassandra::Consistency::QUORUM,
          :all    => Cassandra::Consistency::ALL,
          :any    => Cassandra::Consistency::ANY,
        }
      else
        THRIFT_LEVELS = {
          :one    => HConsistencyLevel::ONE,
          :quorum => HConsistencyLevel::QUORUM,
          :all    => HConsistencyLevel::ALL,
          :any    => HConsistencyLevel::ANY,
        }
      end

      def thrift_read_consistency
        THRIFT_LEVELS[read_consistency] || (raise "Invalid consistency level #{read_consistency}")
      end

      def thrift_write_consistency
        THRIFT_LEVELS[write_consistency] || (raise "Invalid consistency level #{write_consistency}")
      end
    end
  end
end
