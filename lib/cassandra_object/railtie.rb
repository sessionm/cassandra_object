module CassandraObject
  class Railtie < Rails::Railtie
    config.app_middleware.insert_after "::ActionDispatch::Callbacks",
      "CassandraObject::ConnectionAdapters::ConnectionManagement"

    rake_tasks do
      load 'cassandra_object/tasks/ks.rake'
    end

    generators do
      require 'cassandra_object/generators/migration_generator'
    end
  end
end
