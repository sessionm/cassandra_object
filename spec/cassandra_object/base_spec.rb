require 'spec_helper'

describe CassandraObject::Base do
  it "should be able to a create/fetch/destroy an issue" do
    issue = Issue.create! :description => 'web site not working', :worth => 1.5
    expect(issue.persisted?).to be true
    issue = Issue.find issue.id
    expect(issue.worth.to_f).to be 1.5
    issue.destroy
    expect(Issue.find_by_id(issue.id)).to be nil
  end

  it "should be able to get the first issue" do
    issue = Issue.create! :description => 'web site not working', :worth => 1.5
    expect(Issue.first.id).to eq issue.id
  end

  it "should be able to get all issues" do
    issue1 = Issue.create! :description => 'web site not working', :worth => 1.5
    issue2 = Issue.create! :description => 'button is disabled', :worth => 0.2
    expect(Issue.all.map(&:id).sort).to eq [issue1.id, issue2.id].sort
  end

  it "should be able to find issues by id" do
    issue1 = Issue.create! :description => 'web site not working', :worth => 1.5
    issue2 = Issue.create! :description => 'button is disabled', :worth => 0.2
    issue3 = Issue.create! :description => 'button is disabled', :worth => 0.2
    expect(Issue.find_with_ids(issue1.id, issue2.id).map(&:id).sort).to eq [issue1.id, issue2.id].sort
  end

  it "should be able to get all issues" do
    issue1 = Issue.create! :description => 'web site not working', :worth => 1.5
    issue2 = Issue.create! :description => 'button is disabled', :worth => 0.2
    expect(Issue.find_with_ids([issue1.id, issue2.id]).map(&:id).sort).to eq [issue1.id, issue2.id].sort
  end
end
