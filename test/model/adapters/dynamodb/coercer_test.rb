require 'test_helper'
require 'multi_json'

describe Lotus::Model::Adapters::Dynamodb::Coercer do
  before do
    MockEntity = Struct.new(:id) do
      include Lotus::Entity
    end

    class MockCollection
      def attributes
        { id: [Time, :id] }
      end

      def identity
        :id
      end

      def entity
        MockEntity
      end
    end

    @coercer = Lotus::Model::Adapters::Dynamodb::Coercer.new(MockCollection.new)
  end

  after do
    Object.send(:remove_const, :MockEntity)
  end

  describe '#from_*' do
    describe 'skipped' do
      describe 'Float' do
        let(:subject) { 1.5 }

        it 'remains unchanged' do
          @coercer.from_float(subject).class.must_equal Float
          @coercer.from_float(subject).must_equal subject
        end
      end

      describe 'Integer' do
        let(:subject) { 2 }

        it 'remains unchanged' do
          @coercer.from_integer(subject).class.must_equal Fixnum
          @coercer.from_integer(subject).must_equal subject
        end
      end

      describe 'Set' do
        let(:subject) { Set.new([1, 2, 3]) }

        it 'remains unchanged' do
          @coercer.from_set(subject).class.must_equal Set
          @coercer.from_set(subject).must_equal subject
        end
      end

      describe 'String' do
        let(:subject) { "omg" }

        it 'remains unchanged' do
          @coercer.from_string(subject).class.must_equal String
          @coercer.from_string(subject).must_equal subject
        end
      end
    end

    describe 'supported' do
      describe 'AWS::DynamoDB::Binary' do
        let(:subject) { AWS::DynamoDB::Binary.new("HUUUGE") }

        it 'coerces' do
          @coercer.from_aws_dynamodb_binary(subject).class.must_equal \
            AWS::DynamoDB::Binary
          @coercer.from_aws_dynamodb_binary(subject).must_equal \
            AWS::DynamoDB::Binary.new(subject)
        end
      end

      describe 'Array' do
        let(:subject) { ["omg"] }

        it 'coerces' do
          @coercer.from_array(subject).class.must_equal String
          @coercer.from_array(subject).must_equal MultiJson.dump(subject)
        end
      end

      describe 'Boolean' do
        it 'coerces' do
          @coercer.from_boolean(true).class.must_equal Fixnum
          @coercer.from_boolean(true).must_equal 1
          @coercer.from_boolean(false).class.must_equal Fixnum
          @coercer.from_boolean(false).must_equal 0
        end
      end

      describe 'Date' do
        let(:subject) { Date.new(2014) }

        it 'coerces' do
          @coercer.from_date(subject).class.must_equal Fixnum
          @coercer.from_date(subject).must_equal subject.to_time.to_i
        end
      end

      describe 'DateTime' do
        let(:subject) { DateTime.new(2014) }

        it 'coerces' do
          @coercer.from_datetime(subject).class.must_equal Float
          @coercer.from_datetime(subject).must_equal subject.to_time.to_f
        end
      end

      describe 'Hash' do
        let(:subject) { { omg: "lol" } }

        it 'coerces' do
          @coercer.from_hash(subject).class.must_equal String
          @coercer.from_hash(subject).must_equal MultiJson.dump(subject)
        end
      end

      describe 'Time' do
        let(:subject) { Time.at(0) }

        it 'coerces' do
          @coercer.from_time(subject).class.must_equal Float
          @coercer.from_time(subject).must_equal subject.to_f
        end
      end
    end
  end

  describe '#to_*' do
    describe 'skipped' do
      describe 'Float' do
        let(:subject) { 1.5 }

        it 'remains unchanged' do
          @coercer.to_float(subject).must_equal subject
        end
      end

      describe 'Integer' do
        let(:subject) { 2 }

        it 'remains unchanged' do
          @coercer.to_integer(subject).must_equal subject
        end
      end

      describe 'Set' do
        let(:subject) { Set.new([1, 2, 3]) }

        it 'remains unchanged' do
          @coercer.to_set(subject).class.must_equal Set
          @coercer.to_set(subject).must_equal subject
        end
      end

      describe 'String' do
        let(:subject) { "omg" }

        it 'remains unchanged' do
          @coercer.to_string(subject).must_equal subject
        end
      end
    end

    describe 'supported' do
      describe 'AWS::DynamoDB::Binary' do
        let(:subject) { "HUUUGE" }

        it 'coerces' do
          @coercer.to_aws_dynamodb_binary(subject).class.must_equal \
            AWS::DynamoDB::Binary
          @coercer.to_aws_dynamodb_binary(subject).must_equal \
            AWS::DynamoDB::Binary.new(subject)
        end
      end

      describe 'Array' do
        let(:subject) { MultiJson.dump(["omg"]) }

        it 'coerces' do
          @coercer.to_array(subject).class.must_equal Array
          @coercer.to_array(subject).must_equal MultiJson.load(subject)
        end
      end

      describe 'Boolean' do
        it 'coerces' do
          @coercer.to_boolean(1).class.must_equal TrueClass
          @coercer.to_boolean(1).must_equal true
          @coercer.to_boolean(0).class.must_equal FalseClass
          @coercer.to_boolean(0).must_equal false
        end
      end

      describe 'Date' do
        let(:subject) { Date.new(2014) }

        it 'coerces' do
          @coercer.to_date(subject.to_time.to_i).class.must_equal Date
          @coercer.to_date(subject.to_time.to_i).must_equal subject
        end
      end

      describe 'DateTime' do
        let(:subject) { DateTime.new(2014) }

        it 'coerces' do
          @coercer.to_datetime(subject.to_time.to_f).class.must_equal DateTime
          @coercer.to_datetime(subject.to_time.to_f).must_equal subject
        end
      end

      describe 'Hash' do
        let(:subject) { MultiJson.dump({ omg: "lol" }) }

        it 'coerces' do
          @coercer.to_hash(subject).class.must_equal Hash
          @coercer.to_hash(subject).must_equal MultiJson.load(subject)
        end
      end

      describe 'Time' do
        let(:subject) { Time.at(0) }

        it 'coerces' do
          @coercer.to_time(subject.to_f).class.must_equal Time
          @coercer.to_time(subject.to_f).must_equal subject
        end
      end
    end
  end

  describe '#deserialize_*' do
    it 'deserializes id' do
      @coercer.deserialize_id(0.0).must_equal Time.at(0)
    end
  end

  describe '#serialize_*' do
    it 'serializes id' do
      @coercer.serialize_id(Time.at(0)).must_equal 0.0
    end
  end

  describe '#to_record' do
    let(:subject) { MockEntity.new(id: Time.at(0)) }

    it 'serializes entity' do
      @coercer.to_record(subject).must_equal ({ id: 0.0 })
    end
  end

  describe '#from_record' do
    let(:subject) { { id: 1.0 } }

    it 'deserializes entity' do
      @coercer.from_record(subject).must_equal MockEntity.new(id: Time.at(1))
    end
  end
end
