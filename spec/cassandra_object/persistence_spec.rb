require 'spec_helper'

describe CassandraObject::Persistence do
  context "add" do
    it "should increment the counter" do
      id = SimpleUUID::UUID.new.to_guid
      Counter.add id, 1, 'foo'
      expect(Counter.get_counter(id, 'foo')).to eq 1
      Counter.connection.add Counter.column_family, id, 1, ['foo', 'bar']
      expect(Counter.get_counter(id, 'foo')).to eq 2
      expect(Counter.get_counter(id, 'bar')).to eq 1
    end
  end

  context "add_multiple_columns" do
    it "should increment the counter" do
      id = SimpleUUID::UUID.new.to_guid
      Counter.connection.add_multiple_columns Counter.column_family, id, {'foo' => 1}
      expect(Counter.get_counter(id, 'foo')).to eq 1
      Counter.connection.add_multiple_columns Counter.column_family, id, {'foo' => 1, 'bar' => 1}
      expect(Counter.get_counter(id, 'foo')).to eq 2
      expect(Counter.get_counter(id, 'bar')).to eq 1
    end
  end
end
