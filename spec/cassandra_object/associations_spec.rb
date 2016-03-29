require 'spec_helper'

describe CassandraObject::Associations do
  context "belongs_to" do
    it "should fetch the user record from the belongs_to relationship from a cassandra object" do
      user = User.create! :position => 1
      issue = Issue.create! :description => 'web site not working', :worth => 1.5, :user_id => user.id
      expect(issue.user.id).to eq user.id
    end

    it "should fetch the cassandra object from the belongs_to relationship from an active record object" do
      issue = Issue.create! :description => 'web site not working', :worth => 1.5
      user = User.create! :position => 1, :issue_id => issue.id
      role = Role.create! :name => 'admin', :user_id => user.id

      expect(role.user.id).to eq user.id
      expect(user.issue.id).to eq issue.id
    end

    it "should set the foreign key attribute when assigned through the belongs_to relationship" do
      issue = Issue.create! :description => 'web site not working', :worth => 1.5
      user = User.create! :position => 1, :issue => issue
      expect(user.issue_id).to eq issue.id

      user.issue = issue
      expect(user.issue_id).to eq issue.id
    end
  end
end
