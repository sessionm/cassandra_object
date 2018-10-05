# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = 'sessionm-cassandra_object'
  s.version = '4.1.0.beta'
  s.description = 'Cassandra ActiveModel'
  s.summary = 'Cassandra ActiveModel'

  s.authors = ["Matthew Keith", "sessionm"]
  s.email = 'mkeith@sessionm.com'
  s.homepage = 'http://github.com/sessionm/cassandra_object'

  s.extra_rdoc_files = ["README.markdown"]
  s.files       = `git ls-files`.split("\n")
  s.test_files  = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.require_paths = ['lib']
end

