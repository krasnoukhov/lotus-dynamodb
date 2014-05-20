#!/usr/bin/env ruby

#
# Link to original gist: https://gist.github.com/jodosha/17a8bd1a49899753f617
#

require 'benchmark'
require 'benchmark/ips'
require 'lotus/model'
require 'lotus-dynamodb'

GC.disable
TIMES = ENV['TIMES'].to_i || 1_000_000

class Project
  include Lotus::Entity
  self.attributes = :attr1, :attr2, :attr3, :attr4,
    :attr5, :attr6, :attr7, :attr8, :attr9
end

default_collection = Lotus::Model::Mapping::Collection.new(:projects, Lotus::Model::Mapping::Coercer) do
  entity Project

  attribute :id,    Integer
  attribute :attr1, String
  attribute :attr2, String
  attribute :attr3, String
  attribute :attr4, String
  attribute :attr5, String
  attribute :attr6, String
  attribute :attr7, String
  attribute :attr8, String
  attribute :attr9, String
end

dynamodb_collection = Lotus::Model::Mapping::Collection.new(:projects, Lotus::Model::Adapters::Dynamodb::Coercer) do
  entity Project

  attribute :id,    Integer
  attribute :attr1, String
  attribute :attr2, String
  attribute :attr3, String
  attribute :attr4, String
  attribute :attr5, String
  attribute :attr6, String
  attribute :attr7, String
  attribute :attr8, String
  attribute :attr9, String
end

record = Hash[id: '23', attr1: 'attr1', attr2: 'attr2',
              attr3: 'attr3', attr4: 'attr4', attr5: 'attr5',
              attr6: 'attr6', attr7: 'attr7', attr8: 'attr8',
              attr9: 'attr9']

default_collection.load!
dynamodb_collection.load!

Benchmark.bm(30) do |bm|
  bm.report 'default' do
    TIMES.times do
      default_collection.deserialize([record])
    end
  end

  bm.report 'dynamodb' do
    TIMES.times do
      dynamodb_collection.deserialize([record])
    end
  end
end

Benchmark.ips do |x|
  x.report('default') { default_collection.deserialize([record]) }
  x.report('dynamodb') { dynamodb_collection.deserialize([record]) }
end
