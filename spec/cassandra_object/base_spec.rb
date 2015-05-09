require 'spec_helper'

describe CassandraObject::Base do
  it "should be able to a new entry" do
    issue = Issue.create! :description => 'web site not working', :worth => 1.5
    expect(issue.persisted?).to be true
    issue = Issue.find issue.id
    expect(issue.worth.to_f).to be 1.5
  end
end
