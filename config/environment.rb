require File.expand_path('../boot', __FILE__)
require 'pathname'

ENV['RAILS_ENV'] ||= 'development'

Bundler.require :default, ENV['RAILS_ENV']

BASE_DIR = Pathname.new(File.expand_path('../..', __FILE__))

CASSANDRA_YAML = "#{BASE_DIR}/config/cassandra.yml"

DEFAULT_LOG_FILE = "#{BASE_DIR}/log/#{ENV['RAILS_ENV']}.log"

$LOAD_PATH << File.expand_path('../../lib', __FILE__)
require 'cassandra_object'

CassandraObject::Base.config = YAML.load_file(CASSANDRA_YAML)[ENV['RAILS_ENV']]
