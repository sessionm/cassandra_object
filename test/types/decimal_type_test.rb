require File.join(File.dirname(__FILE__), '..', 'test_helper')

class CassandraObject::Types::DecimalTypeTest < CassandraObject::TestCase
  test 'encode' do
    assert_equal '3.14', CassandraObject::Types::DecimalType.encode(BigDecimal.new('3.14'))
  end

  test 'decode' do
    assert_equal BigDecimal.new('0.001'), CassandraObject::Types::DecimalType.decode('.001')
  end
end
