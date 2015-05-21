require 'rubygems'

ENV["RAILS_ENV"] = 'test'

require File.expand_path('../../config/environment', __FILE__)

Bundler.require :default, :test

KEYSPACE = 'cassandra_object_test'

CassandraObject::Adapters::CassandraDriver.new(CassandraObject::Base.connection_spec).cluster.tap do |cluster|
  cluster.connect.tap do |session|
    session.execute("DROP KEYSPACE #{KEYSPACE}") if cluster.has_keyspace?(KEYSPACE)
    replication = Cassandra::Keyspace::Replication.new('org.apache.cassandra.locator.SimpleStrategy', 'replication_factor' => 1)
    keyspace = Cassandra::Keyspace.new(KEYSPACE, false, replication, [], [])
    session.execute keyspace.to_cql
    session.close
  end

  cluster.connect(KEYSPACE).tap do |session|
    session.execute <<-CQL
CREATE TABLE "Issues" (
  key blob,
  column1 text,
  value text,
  PRIMARY KEY (key, column1)
)
CQL

    session.execute <<-CQL
CREATE TABLE "Counters" (
  key blob,
  column1 text,
  value counter,
  PRIMARY KEY (key, column1)
)
CQL
    session.close
  end

  cluster.close
end

ActiveRecord::Base.configurations = YAML.load_file('config/database.yml')
ActiveRecord::Base.establish_connection ENV["RAILS_ENV"]
ActiveRecord::Base.logger = Logger.new(STDOUT)
ActiveRecord::Migrator.migrate('spec/support/db/migrate')

Dir[BASE_DIR.join("spec/support/**/*.rb")].each { |f| require f }

RSpec.configure do |config|

  config.before(:each) do
    Cassandra::Session.delete_all_populated_column_families
  end

  config.after(:all) do
    # we need to call this at the end of each set of tests because if the last test in an rspec file inserts data
    #    into Cassandra it will not be cleaned up at the beginning of the next test in the next rspec file.  This was
    #    first spotted in ref #361.  The test run in autotest called before_create on UserTransaction and added that
    #    class to @@populated_column_families, but when you run an individual file after autotest @@populated_column_families
    #    is empty, so Cassandra doesn't get cleaned up properly...
    Cassandra::Session.delete_all_populated_column_families
  end
end
