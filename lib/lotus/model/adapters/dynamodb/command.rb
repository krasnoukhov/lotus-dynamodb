module Lotus
  module Model
    module Adapters
      module Dynamodb
        # Execute a command for the given collection.
        #
        # @see Lotus::Model::Adapters::Dynamodb::Collection
        # @see Lotus::Model::Mapping::Collection
        #
        # @api private
        # @since 0.1.0
        class Command
          # Initialize a command.
          #
          # @param dataset [Lotus::Model::Adapters::Dynamodb::Collection]
          # @param collection [Lotus::Model::Mapping::Collection]
          #
          # @return [Lotus::Model::Adapters::Dynamodb::Command]
          #
          # @api private
          # @since 0.1.0
          def initialize(dataset, collection)
            @dataset, @collection = dataset, collection
          end

          # Creates a record for the given entity.
          #
          # @param entity [Object] the entity to persist
          #
          # @see Lotus::Model::Adapters::Dynamodb::Collection#create
          #
          # @return the primary key of the just created record.
          #
          # @api private
          # @since 0.1.0
          def create(entity)
            @dataset.create(
              _serialize(entity)
            )
          end

          # Updates the corresponding record for the given entity.
          #
          # @param entity [Object] the entity to persist
          #
          # @see Lotus::Model::Adapters::Dynamodb::Collection#update
          #
          # @api private
          # @since 0.1.0
          def update(entity)
            @dataset.update(
              _serialize(entity)
            )
          end

          # Deletes the corresponding record for the given entity.
          #
          # @param entity [Object] the entity to delete
          #
          # @see Lotus::Model::Adapters::Dynamodb::Collection#delete
          #
          # @api private
          # @since 0.1.0
          def delete(entity)
            @dataset.delete(
              _serialize(entity)
            )
          end

          # Returns an unique record from the given collection, with the given
          # id.
          #
          # @param key [Array] the identity of the object
          #
          # @see Lotus::Model::Adapters::Dynamodb::Collection#get
          #
          # @return [Object] the entity
          #
          # @api private
          # @since 0.1.0
          def get(key)
            @collection.deserialize(
              [@dataset.get(key)].compact
            ).first
          end

          private
          # Serialize the given entity before to persist in the database.
          #
          # @param entity [Object] the entity
          #
          # @return [Hash] the serialized entity
          #
          # @api private
          # @since 0.1.0
          def _serialize(entity)
            serialized = @collection.serialize(entity)
            serialized.delete_if { |_, v| v.nil? }
          end
        end
      end
    end
  end
end
