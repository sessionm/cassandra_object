module CassandraObject
  module Consistency
    extend ActiveSupport::Concern

    included do
      class_attribute :write_consistency
      class_attribute :read_consistency
    end

    module ClassMethods
      THRIFT_LEVELS = {
        :one    => :one,
        :quorum => :quorum,
        :local_quorum => :local_quorum,
        :each_quorum => :each_quorum,
        :all    => :all
      }
      
      DEFAULT_OPTIONS = {
        :read_default => :quorum,
        :write_default => :quorum,
      }

      @@default_read_consistency = DEFAULT_OPTIONS[:read_default]
      @@default_write_consistency = DEFAULT_OPTIONS[:write_default]
      def set_default_consistencies(config)
        config = (config[:consistency] || {}).reverse_merge(DEFAULT_OPTIONS)
        @@default_read_consistency = config[:read_default].to_sym
        @@default_write_consistency = config[:write_default].to_sym
      end

      def thrift_read_consistency
        consistency = read_consistency || @@default_read_consistency
        THRIFT_LEVELS[consistency] || (raise "Invalid consistency level #{consistency}")
      end

      def thrift_write_consistency
        consistency = write_consistency || @@default_write_consistency
        THRIFT_LEVELS[consistency] || (raise "Invalid consistency level #{consistency}")
      end
    end
  end
end
