class Counter < CassandraObject::Base
  self.write_consistency = :all
end
