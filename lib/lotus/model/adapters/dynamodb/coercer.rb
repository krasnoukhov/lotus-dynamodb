require 'lotus/model/mapping/coercions'
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
          SKIPPED_KLASSES = [Float, Integer, Set, String]
          SUPPORTED_KLASSES = [AWS::DynamoDB::Binary, Array, Boolean, Date, DateTime, Hash, Time]

          # Converts value from given type to DynamoDB record value.
          #
          # @api private
          # @since 0.1.0
          def from_aws_dynamodb_binary(value)
            return value if value.nil? || value.is_a?(AWS::DynamoDB::Binary)
            AWS::DynamoDB::Binary.new(value)
          end

          alias_method :to_aws_dynamodb_binary, :from_aws_dynamodb_binary

          # Converts value from given type to DynamoDB record value.
          #
          # @api private
          # @since 0.1.0
          def from_array(value)
            return if value.nil?
            _serialize(value)
          end

          # Converts value from DynamoDB record value to given type.
          #
          # @api private
          # @since 0.1.0
          def to_array(value)
            return if value.nil?
            _deserialize(value)
          end

          # Converts value from given type to DynamoDB record value.
          #
          # @api private
          # @since 0.1.0
          def from_boolean(value)
            return if value.nil?
            value ? 1 : 0
          end

          # Converts value from DynamoDB record value to given type.
          #
          # @api private
          # @since 0.1.0
          def to_boolean(value)
            return if value.nil?
            value.to_i == 1
          end

          # Converts value from given type to DynamoDB record value.
          #
          # @api private
          # @since 0.1.0
          def from_date(value)
            return if value.nil?
            value.to_time.to_i
          end

          # Converts value from DynamoDB record value to given type.
          #
          # @api private
          # @since 0.1.0
          def to_date(value)
            return if value.nil?
            Time.at(value.to_i).to_date
          end

          # Converts value from given type to DynamoDB record value.
          #
          # @api private
          # @since 0.1.0
          def from_datetime(value)
            return if value.nil?
            value.to_time.to_f
          end

          # Converts value from DynamoDB record value to given type.
          #
          # @api private
          # @since 0.1.0
          def to_datetime(value)
            return if value.nil?
            Time.at(value.to_f).to_datetime
          end

          # Converts value from given type to DynamoDB record value.
          #
          # @api private
          # @since 0.1.0
          def from_hash(value)
            return if value.nil?
            _serialize(value)
          end

          # Converts value from DynamoDB record value to given type.
          #
          # @api private
          # @since 0.1.0
          def to_hash(value)
            return if value.nil?
            _deserialize(value)
          end

          # Converts value from given type to DynamoDB record value.
          #
          # @api private
          # @since 0.1.0
          def from_time(value)
            return if value.nil?
            value.to_f
          end

          # Converts value from DynamoDB record value to given type.
          #
          # @api private
          # @since 0.1.0
          def to_time(value)
            return if value.nil?
            Time.at(value.to_f)
          end

          private
          # Compile itself for performance boost.
          #
          # @api private
          # @since 0.1.0
          def _compile!
            _compile_skipped!
            _compile_record!
            _compile_serialization!
          end

          # Compile skipped klasses methods.
          #
          # @api private
          # @since 0.1.1
          def _compile_skipped!
            instance_eval(SKIPPED_KLASSES.map do |klass|
              %{
              def from_#{_method_name(klass)}(value)
                value
              end

              def to_#{_method_name(klass)}(value)
                value
              end
              }
            end.join("\n"))
          end

          # Compile record methods.
          #
          # @api private
          # @since 0.1.1
          def _compile_record!
            instance_eval %{
              def to_record(entity)
                if entity.id
                  Hash[*[#{ @collection.attributes.map{|name,(klass,mapped)| ":#{mapped},from_#{_method_name(klass)}(entity.#{name})"}.join(',') }]]
                else
                  Hash[*[#{ @collection.attributes.reject{|name,_| name == @collection.identity }.map{|name,(klass,mapped)| ":#{mapped},from_#{_method_name(klass)}(entity.#{name})"}.join(',') }]]
                end
              end

              def from_record(record)
                #{ @collection.entity }.new(
                  Hash[*[#{ @collection.attributes.map{|name,(klass,mapped)| ":#{name},#{coercions_wrap(klass) { "to_#{_method_name(klass)}(record[:#{mapped}])" }}"}.join(',') }]]
                )
              end
            }
          end

          # Compile deserialise/serialize methods.
          #
          # @api private
          # @since 0.1.1
          def _compile_serialization!
            instance_eval(@collection.attributes.map do |_,(klass,mapped)|
              %{
              def deserialize_#{ mapped }(value)
                #{coercions_wrap(klass) { "from_#{_method_name(klass)}(value)" }}
              end

              def serialize_#{ mapped }(value)
                from_#{_method_name(klass)}(value)
              end
              }
            end.join("\n"))
          end

          # Wraps string in Lotus::Model::Mapping::Coercions call if needed.
          #
          # @api private
          # @since 0.1.0
          def coercions_wrap(klass)
            if klass.to_s.include?("::")
              yield
            else
              "Lotus::Model::Mapping::Coercions.#{klass}(#{yield})"
            end
          end

          # Returns method name from klass.
          #
          # @api private
          # @since 0.1.0
          def _method_name(klass)
            klass.to_s.downcase.gsub("::", "_")
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
