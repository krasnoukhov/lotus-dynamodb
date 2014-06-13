require 'rubygems'
require 'bundler/setup'

if ENV['COVERAGE'] == 'true'
  require 'simplecov'
  require 'coveralls'

  SimpleCov.formatter = SimpleCov::Formatter::MultiFormatter[
    SimpleCov::Formatter::HTMLFormatter,
    Coveralls::SimpleCov::Formatter
  ]

  SimpleCov.start do
    command_name 'test'
    add_filter   'test'
  end
end

require 'minitest/autorun'
$:.unshift 'lib'
require 'lotus-dynamodb'

if ENV['AWS']
  AWS.config(
    # logger: Logger.new($stdout),
    # log_level: :debug,
    access_key_id: ENV['AWS_KEY'],
    secret_access_key: ENV['AWS_SECRET'],
  )
else
  AWS.config(
    # logger: Logger.new($stdout),
    # log_level: :debug,
    use_ssl: false,
    dynamo_db_endpoint: 'localhost',
    dynamo_db_port: 4567,
    access_key_id: '',
    secret_access_key: '',
  )

  Net::HTTP.new('localhost', AWS.config.dynamo_db_port).delete('/')
end

def skip_for_fake_dynamo
  skip('fake_dynamo does not support this yet') unless ENV['AWS']
end

require 'fixtures'
