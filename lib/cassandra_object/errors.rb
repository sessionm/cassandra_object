module CassandraObject
  class CassandraObjectError < StandardError
  end

  class RecordNotSaved < CassandraObjectError
  end
  
  class RecordNotFound < CassandraObjectError
  end

  class InvalidKey < CassandraObjectError
  end
end
