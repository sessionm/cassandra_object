class Issue < CassandraObject::Base
  key :uuid
  attribute :description, :type => :string
  attribute :worth, :type => :decimal, :precision => 100
  attribute :name, :type => :string
  before_validation :set_defaults, :on => :create

  def set_defaults
    self.name ||= 'default name'
  end
end
