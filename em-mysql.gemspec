spec = Gem::Specification.new do |s|
  s.name = 'em-mysql'
  s.version = '0.2.0'
  s.date = '2009-02-18'
  s.summary = 'Async MySQL client API for Ruby/EventMachine'
  s.email = "em-mysql@tmm1.net"
  s.homepage = "http://github.com/tmm1/em-mysql"
  s.description = 'Async MySQL client API for Ruby/EventMachine'
  s.has_rdoc = false
  s.authors = ["Aman Gupta"]
  s.add_dependency('eventmachine', '>= 0.12.4')

  # git ls-files
  s.files = %w[
    README
    lib/em/mysql.rb
    lib/sequel/async.rb
    test.rb
  ]
end
