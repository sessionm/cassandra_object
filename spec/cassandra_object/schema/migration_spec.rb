require 'spec_helper'

describe CassandraObject::Schema::Migration do
  context "create_column_family" do
    it "should create a new standard column family" do
      expect(Issue.connection.column_families['Foobars'].present?).to be false
      CassandraObject::Schema::Migration.create_column_family("Foobars") do |cf|
        cf.column_type = 'Standard'
        cf.comparator_type = 'UTF8Type'
        cf.default_validation_class = 'UTF8Type'
      end
      expect(Issue.connection.column_families['Foobars'].present?).to be true
    end

    it "should create a new counter column family" do
      expect(Issue.connection.column_families['FooCounters'].present?).to be false
      CassandraObject::Schema::Migration.create_column_family("FooCounters") do |cf|
        cf.column_type = 'Counter'
        cf.comparator_type = 'UTF8Type'
        cf.default_validation_class = 'UTF8Type'
      end
      expect(Issue.connection.column_families['FooCounters'].present?).to be true
    end
  end
end
