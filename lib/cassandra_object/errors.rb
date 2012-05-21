module CassandraObject
  class CasssandraObjectError < StandardError
  end

  class RecordNotSaved < CasssandraObjectError
  end
  
  class RecordNotFound < CasssandraObjectError
  end

  class InvalidKey < CasssandraObjectError
  end
end
