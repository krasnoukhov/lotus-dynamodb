# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'lotus/dynamodb/version'

Gem::Specification.new do |spec|
  spec.name          = 'lotus-dynamodb'
  spec.version       = Lotus::Dynamodb::VERSION
  spec.authors       = ['Dmitry Krasnoukhov']
  spec.email         = ['dmitry@krasnoukhov.com']
  spec.summary       = spec.description = %q{Amazon DynamoDB adapter for Lotus::Model}
  spec.homepage      = 'https://github.com/krasnoukhov/lotus-dynamodb'
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  spec.add_runtime_dependency 'lotus-model', '~> 0.1'
  spec.add_runtime_dependency 'aws-sdk',     '~> 1.0'
  spec.add_runtime_dependency 'multi_json',  '~> 1.10'

  spec.add_development_dependency 'bundler',       '~> 1.5'
  spec.add_development_dependency 'minitest',      '~> 5'
  spec.add_development_dependency 'minitest-line', '~> 0.6'
  spec.add_development_dependency 'rake',          '~> 10'
  spec.add_development_dependency 'fake_dynamo',   '~> 0.2'
  spec.add_development_dependency 'foreman',       '~> 0.67'
end
