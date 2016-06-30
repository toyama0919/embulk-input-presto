
Gem::Specification.new do |spec|
  spec.name          = "embulk-input-presto"
  spec.version       = "0.2.0"
  spec.authors       = ["toyama0919"]
  spec.summary       = "Facebook Presto input plugin for Embulk"
  spec.description   = "Facebook Presto input plugin for Embulk."
  spec.email         = ["toyama0919@gmail.com"]
  spec.licenses      = ["MIT"]
  spec.homepage      = "https://github.com/toyama0919/embulk-input-presto"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "presto-client"
  spec.add_development_dependency 'embulk', ['>= 0.8.9']
  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
  spec.add_development_dependency 'test-unit'
end
