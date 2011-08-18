module CassandraObject
  class CasssandraObjectError < StandardError
  end

  class RecordNotSaved < CasssandraObjectError
  end
  
  class RecordNotFound < CasssandraObjectError
  end

  class ConnectionNotEstablished < CasssandraObjectError
  end
end
