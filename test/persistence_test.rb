require 'test_helper'

class CassandraObject::PersistenceTest < CassandraObject::TestCase
  test 'persistance inquiries' do
    issue = Issue.new
    assert issue.new_record?
    assert !issue.persisted?

    issue.save
    assert issue.persisted?
    assert !issue.new_record?
  end

  test 'save' do
    issue = Issue.new
    issue.save

    assert_equal issue, Issue.find(issue.id)
  end

  test 'save!' do
    begin
      Issue.validates(:description, presence: true)

      issue = Issue.new(description: 'bad', worth: '9000.00311219')
      issue.save!

      issue = Issue.find(issue.id)
      assert_equal BigDecimal.new('9000.00311219'), issue.worth
      assert_equal [18, 117], issue.worth.precs

      issue = Issue.new
      assert_raise(CassandraObject::RecordInvalid) { issue.save! }
    ensure
      Issue.reset_callbacks(:validate)
    end
  end

  test 'destroy' do
    issue = Issue.create
    issue.destroy
 
    assert issue.destroyed?
    assert !issue.persisted?
    assert !issue.new_record?
  end

  test 'add should work' do
    assert_equal nil, Counter.connection.get(Counter.column_family, 'key', 'column')

    Counter.add('key', 1, 'column')
    assert_equal 1, Counter.connection.get(Counter.column_family, 'key', 'column')

    Counter.add('key', -1, 'column')
    assert_equal 0, Counter.connection.get(Counter.column_family, 'key', 'column')
  end

  test 'add should respect default and class consistency option' do
    begin
      old_default_write_cl = CassandraObject::Consistency::ClassMethods.class_variable_get(:@@default_read_consistency)
      old_class_write_cl = Counter.write_consistency
      CassandraObject::Consistency::ClassMethods.class_variable_set(:@@default_read_consistency, :quorum)
      Counter.write_consistency = nil

      Counter.connection.expects(:add).with(Counter.column_family, 'key', 2, 'column', :consistency => :quorum)
      Counter.add('key', 2, 'column')

      Counter.write_consistency = :local_quorum
      Counter.connection.expects(:add).with(Counter.column_family, 'key', 2, 'column', :consistency => :local_quorum)
      Counter.add('key', 2, 'column')
    ensure
      CassandraObject::Consistency::ClassMethods.class_variable_set(:@@default_read_consistency, old_class_write_cl)
      Counter.write_consistency = old_class_write_cl
    end
  end

  test 'update_attribute' do
    issue = Issue.create
    issue.update_attribute(:description, 'lol')

    assert !issue.changed?
    assert_equal 'lol', issue.description
  end
  
  test 'update_attributes' do
    issue = Issue.create
    issue.update_attributes(description: 'lol')

    assert !issue.changed?
    assert_equal 'lol', issue.description
  end

  test 'update_attributes!' do
    begin
      Issue.validates(:description, presence: true)

      issue = Issue.new(description: 'bad')
      issue.save!
      
      assert_raise CassandraObject::RecordInvalid do
        issue.update_attributes! description: ''
      end
    ensure
      Issue.reset_callbacks(:validate)
    end
  end

  test 'reload' do
    persisted_issue = Issue.create
    fresh_issue = Issue.find(persisted_issue.id)
    fresh_issue.update_attribute(:description, 'say what')

    assert_nil persisted_issue.description
    persisted_issue.reload
    assert_equal 'say what', persisted_issue.description
  end
end
