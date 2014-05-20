require 'bundler'
Bundler.require(:default)

require 'lotus/model'
require 'lotus-dynamodb'

AWS.config(
  use_ssl: false,
  dynamo_db_endpoint: 'localhost',
  dynamo_db_port: 4567,
  access_key_id: '',
  secret_access_key: '',
)

#
# Say, we have a ```purchases``` DynamoDB table
#
# This table stores purchases which are split by a ```region``` and sorted by
# creation time.
# Local secondary index allows sorting records by subtotal, and global index is
# used to retrieve specific records by ```uuid``` attribute, even if we don't
# know a ```region``` of these records.
#

DB = AWS::DynamoDB::Client.new(api_version: Lotus::Dynamodb::API_VERSION)

begin
  DB.describe_table("purchases")
rescue AWS::DynamoDB::Errors::ResourceNotFoundException
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
end

#
# Entity
#

class Purchase
  include Lotus::Entity
  self.attributes = :id, :region, :subtotal, :item_ids, :content, :created_at
end

#
# Repository
#

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

#
# Mapper
#

coercer = Lotus::Model::Adapters::Dynamodb::Coercer
mapper = Lotus::Model::Mapper.new(coercer) do
  collection :purchases do
    entity Purchase

    attribute :id,         String, as: :uuid
    attribute :region,     String
    attribute :subtotal,   Float
    attribute :item_ids,   Set
    attribute :content,    AWS::DynamoDB::Binary
    attribute :created_at, Time

    identity :uuid
  end
end.load!

#
# Adapter
#

PurchaseRepository.adapter = Lotus::Model::Adapters::DynamodbAdapter.new(mapper)

#
# Create some data
#

purchases = [
  { region: "europe", subtotal: 15.0,  item_ids: [1, 2] },
  { region: "europe", subtotal: 10.0,  content: "Huge Blob Here" },
  { region: "usa",    subtotal: 5.0,   item_ids: ["strings", "as", "well"] },
  { region: "asia",   subtotal: 100.0 },
].map do |purchase|
  PurchaseRepository.create(
    Purchase.new(purchase.merge(created_at: Time.new))
  )
end

#
# Perform queries
#

puts "Find by UUID"
puts PurchaseRepository.find_by_uuid(purchases.first.id).inspect
puts

puts "Top by subtotal"
puts PurchaseRepository.top_by_subtotal("europe", 50).map(&:inspect)
puts
