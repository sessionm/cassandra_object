class Issue < CassandraObject::Base
  key :uuid
  attribute :user_id, :type => :integer
  attribute :description, :type => :string
  attribute :worth, :type => :decimal, :precision => 100
  attribute :name, :type => :string
  before_validation :set_defaults, :on => :create

  belongs_to :user

  def set_defaults
    self.name ||= 'default name'
  end
end
