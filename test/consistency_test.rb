require 'test_helper'

class CassandraObject::ConsistencyTest < CassandraObject::TestCase
  class TestModel < CassandraObject::Base
  end

  test 'thrift_write_consistency' do
    TestModel.write_consistency = :all
    assert_equal Cassandra::Consistency::ALL, TestModel.thrift_write_consistency
  end
  
  test 'thrift_read_consistency' do
    TestModel.read_consistency = :all
    assert_equal Cassandra::Consistency::ALL, TestModel.thrift_read_consistency
  end
end
