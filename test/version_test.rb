require 'test_helper'

describe Lotus::Dynamodb::VERSION do
  it 'returns current version' do
    Lotus::Dynamodb::VERSION.must_equal '0.1.2'
  end
end
