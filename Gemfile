source 'https://rubygems.org'
gemspec

if !ENV['TRAVIS']
  gem 'byebug', require: false, platforms: :ruby if RUBY_VERSION == '2.1.2'
  gem 'yard',   require: false
end

gem 'simplecov', require: false
gem 'coveralls', require: false

# Benchmarking
gem 'benchmark-ips', '~> 1.2'

# Fixes are not merged yet
gem 'fake_dynamo', github: 'krasnoukhov/fake_dynamo'

# Upstream
gem 'lotus-utils', github: 'lotus/utils'
gem 'lotus-model', github: 'lotus/model'
