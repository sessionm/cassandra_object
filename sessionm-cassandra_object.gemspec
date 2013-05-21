# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = 'sessionm-cassandra_object'
  s.version = '2.4.4'
  s.description = 'Cassandra ActiveModel'
  s.summary = 'Cassandra ActiveModel'

  s.required_ruby_version     = '>= 1.9.2'
  s.required_rubygems_version = '>= 1.3.5'

  s.authors = ["Michael Koziarski", "gotime", "sessionm"]
  s.email = 'klange@sessionm.com'
  s.homepage = 'http://github.com/sessionm/cassandra_object'

  s.extra_rdoc_files = ["README.markdown"]
  s.files       = `git ls-files`.split("\n")
  s.test_files  = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.require_paths = ['lib']

  s.add_runtime_dependency('rails', "~> 3.0.9")
  s.add_runtime_dependency('sessionm-cassandra', ">= 1.0.0")

  s.add_development_dependency('bundler', ">= 1.0.0")
end

