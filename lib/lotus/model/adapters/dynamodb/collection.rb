require 'securerandom'
require 'aws-sdk'
require 'lotus/utils/hash'

module Lotus
  module Model
    module Adapters
      module Dynamodb
        # Acts like table, using AWS::DynamoDB::Client.
        #
        # @api private
        # @since 0.1.0
        class Collection
          include AWS::DynamoDB::Types

          # Response interface provides count and entries.
          #
          # @api private
          # @since 0.1.0
          class Response
            attr_accessor :count, :entries

            def initialize(count, entries = nil)
              @count, @entries = count, entries
            end
          end

          # @attr_reader name [String] the name of the collection (eg. `users`)
          #
          # @since 0.1.0
          # @api private
          attr_reader :name

          # @attr_reader identity [Symbol] the primary key of the collection
          #   (eg. `:id`)
          #
          # @since 0.1.0
          # @api private
          attr_reader :identity

          # Initialize a collection.
          #
          # @param client [AWS::DynamoDB::Client] DynamoDB client
          # @param name [Symbol] the name of the collection (eg. `:users`)
          # @param identity [Symbol] the primary key of the collection
          #   (eg. `:id`).
          #
          # @api private
          # @since 0.1.0
          def initialize(client, name, identity)
            @client, @name, @identity = client, name.to_s, identity
            @key_schema = {}
          end

          # Creates a record for the given entity and returns a primary key.
          #
          # @param entity [Object] the entity to persist
          #
          # @see Lotus::Model::Adapters::Dynamodb::Command#create
          # @see http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/DynamoDB/Client/V20120810.html#put_item-instance_method
          #
          # @return the primary key of the just created record.
          #
          # @api private
          # @since 0.1.0
          def create(entity)
            entity[identity] ||= SecureRandom.uuid

            @client.put_item(
              table_name: name,
              item: serialize_item(entity),
            )

            entity[identity]
          end

          # Updates the record corresponding to the given entity.
          #
          # @param entity [Object] the entity to persist
          #
          # @see Lotus::Model::Adapters::Dynamodb::Command#update
          # @see http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/DynamoDB/Client/V20120810.html#update_item-instance_method
          #
          # @api private
          # @since 0.1.0
          def update(entity)
            @client.update_item(
              table_name: name,
              key: serialize_key(entity),
              attribute_updates: serialize_attributes(entity),
            )
          end

          # Deletes the record corresponding to the given entity.
          #
          # @param entity [Object] the entity to delete
          #
          # @see Lotus::Model::Adapters::Dynamodb::Command#delete
          # @see http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/DynamoDB/Client/V20120810.html#delete_item-instance_method
          #
          # @api private
          # @since 0.1.0
          def delete(entity)
            @client.delete_item(
              table_name: name,
              key: serialize_key(entity),
            )
          end

          # Returns an unique record from the given collection, with the given
          # id.
          #
          # @param key [Array] the identity of the object
          #
          # @see Lotus::Model::Adapters::Dynamodb::Command#get
          # @see http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/DynamoDB/Client/V20120810.html#get_item-instance_method
          #
          # @return [Hash] the serialized entity
          #
          # @api private
          # @since 0.1.0
          def get(key)
            return if key.any? { |v| v.to_s == "" }
            return if key.count != key_schema.count

            response = @client.get_item(
              table_name: name,
              key: serialize_key(key),
            )

            deserialize_item(response[:item]) if response[:item]
          end

          # Performs DynamoDB query operation.
          #
          # @param options [Hash] AWS::DynamoDB::Client options
          #
          # @see http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/DynamoDB/Client/V20120810.html#query-instance_method
          #
          # @return [Array<Hash>] the serialized entities
          #
          # @api private
          # @since 0.1.0
          def query(options = {})
            response = @client.query(options.merge(table_name: name))
            deserialize_response(response)
          end

          # Performs DynamoDB scan operation.
          #
          # @param options [Hash] AWS::DynamoDB::Client options
          #
          # @see http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/DynamoDB/Client/V20120810.html#scan-instance_method
          #
          # @return [Array<Hash>] the serialized entities
          #
          # @api private
          # @since 0.1.0
          def scan(options = {})
            response = @client.scan(options.merge(table_name: name))
            deserialize_response(response)
          end

          # Fetches DynamoDB table schema.
          #
          # @see http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/DynamoDB/Client/V20120810.html#describe_table-instance_method
          #
          # @return [Hash] table schema definition
          #
          # @api private
          # @since 0.1.0
          def schema
            @schema ||= @client.describe_table(table_name: name).fetch(:table)
          end

          # Maps table key schema to hash with attribute name as key and key
          # type as value.
          #
          # @param index [String] index to check (defaults to table itself)
          #
          # @see Lotus::Model::Adapters::Dynamodb::Collection#schema
          #
          # @return [Hash] key schema definition
          #
          # @api private
          # @since 0.1.0
          def key_schema(index = nil)
            return @key_schema[index] if @key_schema[index]

            current_schema = if index
              everything = Array(schema[:local_secondary_indexes]) +
                           Array(schema[:global_secondary_indexes])
              indexes = Hash[everything.map { |i| [i[:index_name], i] }]
              indexes[index][:key_schema]
            else
              schema[:key_schema]
            end

            @key_schema[index] ||= Hash[current_schema.to_a.map do |key|
              [key[:attribute_name].to_sym, key[:key_type]]
            end]
          end

          # Checks if given column is in key schema or not.
          #
          # @param column [String] column to check
          # @param index [String] index to check (defaults to table itself)
          #
          # @see Lotus::Model::Adapters::Dynamodb::Collection#key_schema
          #
          # @return [Boolean]
          #
          # @api private
          # @since 0.1.0
          def key?(column, index = nil)
            key_schema(index).has_key?(column)
          end

          # Serialize given entity to have proper attributes for 'item' query.
          #
          # @param entity [Hash] the serialized entity
          #
          # @see AWS::DynamoDB::Types
          #
          # @return [Hash] the serialized item
          #
          # @api private
          # @since 0.1.0
          def serialize_item(entity)
            Hash[entity.map { |k, v| [k.to_s, format_attribute_value(v)] }]
          end

          # Serialize given entity or primary key to have proper attributes
          # for 'key' query.
          #
          # @param entity [Hash,Array] the serialized entity or primary key
          #
          # @see AWS::DynamoDB::Types
          #
          # @return [Hash] the serialized key
          #
          # @api private
          # @since 0.1.0
          def serialize_key(entity)
            Hash[key_schema.keys.each_with_index.map do |k, idx|
              v = entity.is_a?(Hash) ? entity[k] : entity[idx]
              [k.to_s, format_attribute_value(v)]
            end]
          end

          # Serialize given entity to exclude key schema attributes.
          #
          # @param entity [Hash] the serialized entity
          #
          # @see AWS::DynamoDB::Types
          #
          # @return [Hash] the serialized attributes
          #
          # @api private
          # @since 0.1.0
          def serialize_attributes(entity)
            keys = key_schema.keys
            Hash[entity.reject { |k, _| keys.include?(k) }.map do |k, v|
              [k.to_s, { value: format_attribute_value(v), action: "PUT" }]
            end]
          end

          # Deserialize DynamoDB scan/query response.
          #
          # @param response [Hash] the serialized response
          #
          # @return [Hash] the deserialized response
          #
          # @api private
          # @since 0.1.0
          def deserialize_response(response)
            result = Response.new(response[:count])

            result.entries = response[:member].map do |item|
              deserialize_item(item)
            end if response[:member]

            result
          end

          # Deserialize item from DynamoDB response.
          #
          # @param item [Hash] the serialized item
          #
          # @see AWS::DynamoDB::Types
          #
          # @return [Hash] the deserialized entity
          #
          # @api private
          # @since 0.1.0
          def deserialize_item(item)
            Lotus::Utils::Hash.new(values_from_response_hash(item)).symbolize!
          end
        end
      end
    end
  end
end
