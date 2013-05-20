require 'test_helper'

class CassandraObject::AttributesTest < CassandraObject::TestCase

  class A < CassandraObject::Base
    attribute :a, :type => :integer
  end

  class B < CassandraObject::Base
    attribute :b, :type => :integer
  end

  test 'model_attributes' do
    assert_equal %W[created_at updated_at a], A.model_attributes.keys
    assert_equal %W[created_at updated_at b], B.model_attributes.keys
  end

end
