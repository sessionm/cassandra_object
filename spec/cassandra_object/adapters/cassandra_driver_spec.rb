require 'spec_helper'

describe CassandraObject::Adapters::CassandraDriver do
  context "Client" do
    it "should return the column family definition by name" do
      expect(Issue.connection.column_families['Issues'].present?).to be true
      expect(Issue.connection.column_families['Unknowns'].present?).to be false
    end
  end
end
