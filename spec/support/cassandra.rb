require 'cassandra'

module Cassandra
  class Session
    @@populated_column_families = Set.new
    @@test_client = CassandraObject::Adapters::CassandraDriver.new(CassandraObject::Base.connection_spec).client

    def self.truncate_populated_column_family(column_family)
      @@test_client.execute "TRUNCATE \"#{column_family}\""
    end

    def self.delete_all_populated_column_families
      unless @@populated_column_families.empty?
        @@populated_column_families.each { |column_family| self.truncate_populated_column_family(column_family) }
        @@populated_column_families.clear
      end
    end

    [:execute_async].each do |method|
      define_method("#{method}_with_populated_tracking") do |*args|
        send("#{method}_without_populated_tracking", *args).tap do
          if args.first =~ /(insert into|update) "(.+?)"/i
            @@populated_column_families.add $2
          end
        end
      end
      alias_method_chain method, :populated_tracking
    end
  end
end
