# Lotus::Model DynamoDB Adapter

> An adapter is a concrete implementation of persistence logic for a specific
> database.
>
> -- <cite>[jodosha](https://github.com/jodosha), [Lotus::Model](https://github.com/lotus/model)</cite>

This adapter implements persistence layer for a [Amazon DynamoDB](https://aws.amazon.com/dynamodb/),
and it pretends to be a _really_ sane solution to fully experience DynamoDB advantages with Ruby.

It is built using ```AWS::DynamoDB::Client```, which is a part of ```aws-sdk``` gem and implements latest version of DynamoDB protocol.

## Status

[![Gem Version](https://badge.fury.io/rb/lotus-dynamodb.svg)](http://badge.fury.io/rb/lotus-dynamodb)
[![Build Status](https://secure.travis-ci.org/krasnoukhov/lotus-dynamodb.svg?branch=master)](http://travis-ci.org/krasnoukhov/lotus-dynamodb?branch=master)
[![Coverage Status](https://img.shields.io/coveralls/krasnoukhov/lotus-dynamodb.svg)](https://coveralls.io/r/krasnoukhov/lotus-dynamodb?branch=master)
[![Code Climate](https://img.shields.io/codeclimate/github/krasnoukhov/lotus-dynamodb.svg)](https://codeclimate.com/github/krasnoukhov/lotus-dynamodb)
[![Inline docs](http://inch-pages.github.io/github/krasnoukhov/lotus-dynamodb.svg)](http://inch-pages.github.io/github/krasnoukhov/lotus-dynamodb)
[![Dependencies](https://gemnasium.com/krasnoukhov/lotus-dynamodb.svg)](https://gemnasium.com/krasnoukhov/lotus-dynamodb)

## Links

* API Doc: [http://rdoc.info/gems/lotus-dynamodb](http://rdoc.info/gems/lotus-dynamodb)
* Bugs/Issues: [https://github.com/krasnoukhov/lotus-dynamodb/issues](https://github.com/krasnoukhov/lotus-dynamodb/issues)

## Installation

Add this line to your application's Gemfile:

    gem 'lotus-dynamodb'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install lotus-dynamodb

## Usage

Please refer to [Lotus::Model](https://github.com/lotus/model#usage) docs for any details related to Entity, Repository, Data Mapper and Adapter.

### Data types

This adapter supports coercion to all DynamoDB types, including blobs and sets.

List of Ruby types that are supported:

* AWS::DynamoDB::Binary – ```B```
* Array – ```S``` (via MultiJson)
* Boolean – ```N``` (1 for true and 0 for false)
* Date – ```N``` (Integer, seconds since Epoch)
* DateTime – ```N``` (Float, seconds since Epoch)
* Float – ```N```
* Hash – ```S``` (via MultiJson)
* Integer – ```N```
* Set – ```SS```, ```NS```, ```BS``` (Set of String, Number or AWS::DynamoDB::Binary)
* String – ```S```
* Time – ```N``` (Float, seconds since Epoch)

### Repository methods

See [complete list](https://github.com/lotus/model#repositories) of Repository methods provided by ```Lotus::Model```.

Following methods are not supported since it's incompatible with DynamoDB:

* first
* last

### Query methods

Generic methods supported by DynamoDB adapter:

* [all](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#all-instance_method)
* [where](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#where-instance_method) (aliases: ```eq```, ```in```, ```between```)
* [or](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#or-instance_method)
* [exclude](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#exclude-instance_method) (aliases: ```not```, ```ne```)
* [select](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#select-instance_method)
* [order](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#order-instance_method) (alias: ```asc```)
* [desc](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#desc-instance_method)
* [limit](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#limit-instance_method)
* [exists?](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#exist%3F-instance_method) (alias: ```exist?```)
* [count](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#count-instance_method)

DynamoDB-specific methods:

* [query](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#query-instance_method) – ensure ```query``` operation is performed instead of ```scan```
* [consistent](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#consistent-instance_method) – require consistent read for query
* [index](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#index-instance_method) – perform query on specific index
* [le](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#le-instance_method)
* [lt](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#lt-instance_method)
* [ge](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#ge-instance_method)
* [gt](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#gt-instance_method)
* [contains](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#contains-instance_method)
* [not_contains](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#not_contains-instance_method)
* [begins_with](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#begins_with-instance_method)
* [null](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#null-instance_method)
* [not_null](http://rdoc.info/gems/lotus-dynamodb/Lotus/Model/Adapters/Dynamodb/Query#not_null-instance_method)

### Example

Check out the simple example in [examples/purchase.rb](examples/purchase.rb).

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
