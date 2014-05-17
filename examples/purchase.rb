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
# Define
#

class Purchase
  include Lotus::Entity
  self.attributes = :id, :region, :subtotal, :created_at
end

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

PurchaseRepository.adapter = Lotus::Model::Adapters::DynamodbAdapter.new(mapper)

#
# Create
#

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

#
# Query
#

puts "Find by UUID"
puts PurchaseRepository.find_by_uuid(purchases.first.id).inspect
puts

puts "Top by subtotal"
puts PurchaseRepository.top_by_subtotal("europe", 50).map(&:inspect)
puts
