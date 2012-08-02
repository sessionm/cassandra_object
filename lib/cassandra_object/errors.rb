module CassandraObject
  class CassandraObjectError < StandardError
  end

  class RecordNotSaved < CassandraObjectError
  end
  
  class RecordNotFound < CassandraObjectError
  end

  class InvalidKey < CassandraObjectError
  end

  # Raised on attempt to update record that is instantiated as read only.
  class ReadOnlyRecord < CassandraObjectError
  end
end
