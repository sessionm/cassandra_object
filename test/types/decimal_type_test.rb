require File.join(File.dirname(__FILE__), '..', 'test_helper')

class CassandraObject::Types::DecimalTypeTest < CassandraObject::TestCase
  test 'encode' do
    assert_equal nil, CassandraObject::Types::DecimalType.encode(nil)
    assert_equal '3', CassandraObject::Types::DecimalType.encode(3)
    assert_equal '3.14', CassandraObject::Types::DecimalType.encode(3.14)
    assert_equal '3.14', CassandraObject::Types::DecimalType.encode(BigDecimal.new('3.14'))
  end

  test 'decode' do
    assert_equal nil, CassandraObject::Types::DecimalType.decode(nil)
    assert_equal BigDecimal.new('0.001'), CassandraObject::Types::DecimalType.decode(0.001)
    assert_equal BigDecimal.new('0.001'), CassandraObject::Types::DecimalType.decode('.001')
    assert_equal [9, 18], CassandraObject::Types::DecimalType.decode('.001').precs
    assert_equal [9, 27], CassandraObject::Types::DecimalType.decode('.001', :precision => 14).precs
  end
end
