require 'lotus/utils/kernel'

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
          SUPPORTED_KLASSES = [Date, DateTime, Time]
          UNSPPORTED_KLASSES = [Array, Boolean, Hash, Set]

          private
          # Compile itself for performance boost.
          #
          # @api private
          # @since 0.1.0
          def _compile!
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
                raise NotImplementedError, "#{klass} coercion is not supported by DynamoDB"
              end

              def to_#{klass.to_s.downcase}(value)
                raise NotImplementedError, "#{klass} coercion is not supported by DynamoDB"
              end
              }
            end.join("\n"))
          end

          # TODO: Doc
          def from_date(value)
            value.to_time.to_i
          end

          def to_date(value)
            Time.at(value.to_i).to_date
          end

          def from_datetime(value)
            value.to_time.to_f
          end

          def to_datetime(value)
            Time.at(value.to_f).to_datetime
          end

          def from_time(value)
            value.to_f
          end

          def to_time(value)
            Time.at(value.to_f)
          end
        end
      end
    end
  end
end
