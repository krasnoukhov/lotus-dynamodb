# Lotus::Model DynamoDB Adapter

> An adapter is a concrete implementation of persistence logic for a specific
> database.
>
> -- <cite>[jodosha](https://github.com/jodosha), [Lotus::Model](https://github.com/lotus/model)</cite>

This adapter implements persistence layer for a [Amazon DynamoDB](https://aws.amazon.com/dynamodb/),
and it pretends to be a _really_ sane solution to take DynamoDB advantages with Ruby.

It is built on top of ```AWS::DynamoDB::Client```, which is part of ```aws-sdk``` gem and implements latest version of DynamoDB protocol.

## Status

[![Gem Version](https://badge.fury.io/rb/lotus-dynamodb.svg)](http://badge.fury.io/rb/lotus-dynamodb)
[![Build Status](https://secure.travis-ci.org/krasnoukhov/lotus-dynamodb.svg?branch=master)](http://travis-ci.org/krasnoukhov/lotus-dynamodb?branch=master)
[![Coverage Status](https://img.shields.io/coveralls/krasnoukhov/lotus-dynamodb.svg)](https://coveralls.io/r/krasnoukhov/lotus-dynamodb?branch=master)
[![Code Climate](https://img.shields.io/codeclimate/github/krasnoukhov/lotus-dynamodb.svg)](https://codeclimate.com/github/krasnoukhov/lotus-dynamodb)
[![Inline docs](http://inch-pages.github.io/github/krasnoukhov/lotus-dynamodb.svg)](http://inch-pages.github.io/github/krasnoukhov/lotus-dynamodb)
[![Dependencies](https://gemnasium.com/krasnoukhov/lotus-dynamodb.svg)](https://gemnasium.com/krasnoukhov/lotus-dynamodb)

## Installation

Add this line to your application's Gemfile:

    gem 'lotus-dynamodb'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install lotus-dynamodb

## Usage

Please refer to [Lotus::Model](https://github.com/lotus/model#usage) docs for details related to Entities, Repositories, Data Mapper and Adapter things.

#### Example

##### Table
Say, we have a ```purchases``` DynamoDB table:

```
DB = AWS::DynamoDB::Client.new(api_version: Lotus::Dynamodb::API_VERSION)
DB.create_table(
  table_name: "purchases",

  # List of all attributes which are used as table and indexes keys
  attribute_definitions: [
    { attribute_name: "region",       attribute_type: "S" },
    { attribute_name: "created_at",   attribute_type: "N" },
    { attribute_name: "subtotal",     attribute_type: "N" },
    { attribute_name: "uuid",         attribute_type: "S" },
  ],

  # Key schema of table
  key_schema: [
    { attribute_name: "region",       key_type: "HASH" },
    { attribute_name: "created_at",   key_type: "RANGE" },
  ],

  # List of local indexes
  local_secondary_indexes: [{
    index_name: "by_subtotal",
    key_schema: [
      { attribute_name: "region",     key_type: "HASH" },
      { attribute_name: "subtotal",   key_type: "RANGE" },
    ],
    projection: {
      projection_type: "ALL",
    },
  }],

  # List of global indexes
  global_secondary_indexes: [{
    index_name: "by_uuid",
    key_schema: [
      { attribute_name: "uuid",       key_type: "HASH" },
    ],
    projection: {
      projection_type: "ALL",
    },
    provisioned_throughput: {
      read_capacity_units: 10,
      write_capacity_units: 10,
    },
  }],

  # Capacity
  provisioned_throughput: {
    read_capacity_units: 10,
    write_capacity_units: 10,
  },
)
```

This table stores purchases which are split by a ```region``` and sorted by creation time.
Local secondary index allows sorting records by subtotal, and global index is used to retrieve specific records by ```uuid``` attribute, even if we don't know a ```region``` of these records.

##### Entity

```
class Purchase do
  include Lotus::Entity
  self.attributes = :id, :region, :subtotal, :created_at
end
```

##### Repository

```
class PurchaseRepository
  include Lotus::Repository

  class << self
    def find_by_uuid(uuid)
      query do
        index("by_uuid").where(uuid: uuid).limit(1)
      end.all.first
    end

    def top_by_subtotal(region, limit)
      query do
        index("by_subtotal").where(region: region).desc.limit(limit)
      end.all
    end
  end
end
```
##### Mapper

```
mapper = Lotus::Model::Mapper.new do
  collection :purchases do
    entity Purchase

    attribute :id,         String, as: :uuid
    attribute :region,     String
    attribute :subtotal,   Float
    attribute :created_at, Float

    identity :uuid
  end
end.load!
```

##### Adapter :sparkles:

```
require 'lotus-dynamodb'

AWS.config(
  access_key_id: 'your key',
  secret_access_key: 'your secret',
)

PurchaseRepository.adapter = Lotus::Model::Adapters::DynamodbAdapter.new(mapper)
```

##### Create some data

```
purchases = [
  { region: "europe", subtotal: 15.0 },
  { region: "europe", subtotal: 10.0 },
  { region: "usa",    subtotal: 5.0 },
  { region: "asia",   subtotal: 100.0 },
].map do |purchase|
  PurchaseRepository.create(
    Purchase.new(purchase.merge(created_at: Time.new.to_f))
  )
end
```

##### Queries! :fire:

```
puts "Find by UUID"
puts PurchaseRepository.find_by_uuid(purchases.first.id).inspect
puts

puts "Top by subtotal"
puts PurchaseRepository.top_by_subtotal("europe", 5).map(&:inspect)
puts
```

This code is also in [examples/purchase.rb](examples/purchase.rb).

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
