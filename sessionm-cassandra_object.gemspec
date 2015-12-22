# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = 'sessionm-cassandra_object'
  s.version = '4.0.10'
  s.description = 'Cassandra ActiveModel'
  s.summary = 'Cassandra ActiveModel'

  s.authors = ["Michael Koziarski", "gotime", "sessionm"]
  s.email = 'doug@sessionm.com'
  s.homepage = 'http://github.com/sessionm/cassandra_object'

  s.extra_rdoc_files = ["README.markdown"]
  s.files       = `git ls-files`.split("\n")
  s.test_files  = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.require_paths = ['lib']
end

