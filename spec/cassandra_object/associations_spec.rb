require 'spec_helper'

describe CassandraObject::Associations do
  it "should allow for belongs_to relationships" do
    user = User.create! :position => 1
    issue = Issue.create! :description => 'web site not working', :worth => 1.5, :user_id => user.id
    expect(issue.user.id).to eq user.id
  end
end
