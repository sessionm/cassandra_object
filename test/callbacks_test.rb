require 'test_helper'

class CassandraObject::CallbacksTest < CassandraObject::TestCase

  test 'before_validation respects :on' do
    issue = Issue.new
    issue.save
    assert_equal 'default name', issue.name

    # make sure :on => :create is respected
    issue.name = nil
    issue.save
    assert_equal nil, issue.name
  end

end
