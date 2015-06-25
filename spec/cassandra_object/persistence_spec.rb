require 'spec_helper'

describe CassandraObject::Persistence do
  context "add" do
    it "should increment the counter" do
      id = SimpleUUID::UUID.new.to_guid
      Counter.add id, 1, 'foo'
      expect(Counter.get_counter(id, 'foo')).to eq 1
      Counter.add id, 1, 'foo'
      expect(Counter.get_counter(id, 'foo')).to eq 2
    end
  end
end
