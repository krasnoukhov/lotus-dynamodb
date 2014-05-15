DB = AWS::DynamoDB::Client.new(api_version: Lotus::Dynamodb::API_VERSION)

DB.create_table(
  table_name: "test_users",
  attribute_definitions: [
    { attribute_name: "id", attribute_type: "S" },
  ],
  key_schema: [
    { attribute_name: "id", key_type: "HASH" },
  ],
  provisioned_throughput: {
    read_capacity_units: 10,
    write_capacity_units: 10,
  },
)

DB.create_table(
  table_name: "test_devices",
  attribute_definitions: [
    { attribute_name: "uuid",       attribute_type: "S" },
    { attribute_name: "created_at", attribute_type: "N" },
  ],
  key_schema: [
    { attribute_name: "uuid",       key_type: "HASH" },
    { attribute_name: "created_at", key_type: "RANGE" },
  ],
  provisioned_throughput: {
    read_capacity_units: 10,
    write_capacity_units: 10,
  },
)

DB.create_table(
  table_name: "test_purchases",
  attribute_definitions: [
    { attribute_name: "region",       attribute_type: "S" },
    { attribute_name: "created_at",   attribute_type: "N" },
    { attribute_name: "subtotal",     attribute_type: "N" },
    { attribute_name: "uuid",         attribute_type: "S" },
  ],
  key_schema: [
    { attribute_name: "region",       key_type: "HASH" },
    { attribute_name: "created_at",   key_type: "RANGE" },
  ],
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
  provisioned_throughput: {
    read_capacity_units: 10,
    write_capacity_units: 10,
  },
)
