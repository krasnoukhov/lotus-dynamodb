require 'lotus/utils/kernel'
require 'multi_json'

module Lotus
  module Model
    module Adapters
      module Dynamodb
        # Translates values from/to the database with the corresponding Ruby type.
        #
        # @api private
        # @since 0.1.0
        class Coercer < Lotus::Model::Mapping::Coercer
          SKIPPED_KLASSES = [Float, Integer, String]
          SUPPORTED_KLASSES = [Array, Boolean, Date, DateTime, Hash, Time]
          UNSPPORTED_KLASSES = [Set]

          # Converts value from given type to DynamoDB record value.
          #
          # @api private
          # @since 0.1.0
          def from_array(value)
            _serialize(value)
          end

          # Converts value from DynamoDB record value to given type.
          #
          # @api private
          # @since 0.1.0
          def to_array(value)
            _deserialize(value)
          end

          # Converts value from given type to DynamoDB record value.
          #
          # @api private
          # @since 0.1.0
          def from_boolean(value)
            value ? 1 : 0
          end

          # Converts value from DynamoDB record value to given type.
          #
          # @api private
          # @since 0.1.0
          def to_boolean(value)
            value.to_i == 1
          end

          # Converts value from given type to DynamoDB record value.
          #
          # @api private
          # @since 0.1.0
          def from_date(value)
            value.to_time.to_i
          end

          # Converts value from DynamoDB record value to given type.
          #
          # @api private
          # @since 0.1.0
          def to_date(value)
            Time.at(value.to_i).to_date
          end

          # Converts value from given type to DynamoDB record value.
          #
          # @api private
          # @since 0.1.0
          def from_datetime(value)
            value.to_time.to_f
          end

          # Converts value from DynamoDB record value to given type.
          #
          # @api private
          # @since 0.1.0
          def to_datetime(value)
            Time.at(value.to_f).to_datetime
          end

          # Converts value from given type to DynamoDB record value.
          #
          # @api private
          # @since 0.1.0
          def from_hash(value)
            _serialize(value)
          end

          # Converts value from DynamoDB record value to given type.
          #
          # @api private
          # @since 0.1.0
          def to_hash(value)
            _deserialize(value)
          end

          # Converts value from given type to DynamoDB record value.
          #
          # @api private
          # @since 0.1.0
          def from_time(value)
            value.to_f
          end

          # Converts value from DynamoDB record value to given type.
          #
          # @api private
          # @since 0.1.0
          def to_time(value)
            Time.at(value.to_f)
          end

          private
          # Compile itself for performance boost.
          #
          # @api private
          # @since 0.1.0
          def _compile!
            instance_eval(SKIPPED_KLASSES.map do |klass|
              %{
              def from_#{klass.to_s.downcase}(value)
                value
              end

              def to_#{klass.to_s.downcase}(value)
                value
              end
              }
            end.join("\n"))

            instance_eval(UNSPPORTED_KLASSES.map do |klass|
              %{
              def from_#{klass.to_s.downcase}(value)
                raise NotImplementedError, "#{klass} coercion is not supported"
              end

              def to_#{klass.to_s.downcase}(value)
                raise NotImplementedError, "#{klass} coercion is not supported"
              end
              }
            end.join("\n"))

            code = @collection.attributes.map do |_,(klass,mapped)|
              %{
              def deserialize_#{ mapped }(value)
                Lotus::Utils::Kernel.#{klass}(from_#{klass.to_s.downcase}(value))
              end

              def serialize_#{ mapped }(value)
                from_#{klass.to_s.downcase}(value)
              end
              }
            end.join("\n")

            instance_eval %{
              def to_record(entity)
                if entity.id
                  Hash[*[#{ @collection.attributes.map{|name,(klass,mapped)| ":#{mapped},from_#{klass.to_s.downcase}(entity.#{name})"}.join(',') }]]
                else
                  Hash[*[#{ @collection.attributes.reject{|name,_| name == @collection.identity }.map{|name,(klass,mapped)| ":#{mapped},from_#{klass.to_s.downcase}(entity.#{name})"}.join(',') }]]
                end
              end

              def from_record(record)
                #{ @collection.entity }.new(
                  Hash[*[#{ @collection.attributes.map{|name,(klass,mapped)| ":#{name},Lotus::Utils::Kernel.#{klass}(to_#{klass.to_s.downcase}(record[:#{mapped}]))"}.join(',') }]]
                )
              end

              #{ code }
            }
          end

          # Serializes value to string.
          #
          # @api private
          # @since 0.1.0
          def _serialize(value)
            MultiJson.dump(value)
          end

          # Deserializes value from string.
          #
          # @api private
          # @since 0.1.0
          def _deserialize(value)
            MultiJson.load(value)
          end
        end
      end
    end
  end
end
