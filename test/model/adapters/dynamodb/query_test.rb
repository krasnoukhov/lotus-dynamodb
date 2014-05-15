require 'test_helper'

describe Lotus::Model::Adapters::Dynamodb::Query do
  before do
    MockDataset = Struct.new(:records) do
      def query(options = {})
        records
      end

      def scan(options = {})
        records
      end

      def to_s
        records.to_s
      end
    end

    class MockCollection
      def deserialize(array)
        array
      end
    end

    collection = MockCollection.new
    @query     = Lotus::Model::Adapters::Dynamodb::Query.new(dataset, collection)
  end

  after do
    Object.send(:remove_const, :MockDataset)
    Object.send(:remove_const, :MockCollection)
  end

  let(:dataset) { MockDataset.new([]) }

  describe '#negate!' do
    it 'raises an error' do
      -> { @query.negate! }.must_raise NotImplementedError
    end
  end

  describe '#offset' do
    it 'raises an error' do
      -> { @query.offset(1) }.must_raise NotImplementedError
    end
  end

  describe '#sum' do
    it 'raises an error' do
      -> { @query.sum(:id) }.must_raise NotImplementedError
    end
  end

  describe '#average' do
    it 'raises an error' do
      -> { @query.average(:id) }.must_raise NotImplementedError
    end
  end

  describe '#avg' do
    it 'raises an error' do
      -> { @query.avg(:id) }.must_raise NotImplementedError
    end
  end

  describe '#max' do
    it 'raises an error' do
      -> { @query.max(:id) }.must_raise NotImplementedError
    end
  end

  describe '#min' do
    it 'raises an error' do
      -> { @query.min(:id) }.must_raise NotImplementedError
    end
  end

  describe '#interval' do
    it 'raises an error' do
      -> { @query.interval(:id) }.must_raise NotImplementedError
    end
  end

  describe '#range' do
    it 'raises an error' do
      -> { @query.range(:id) }.must_raise NotImplementedError
    end
  end

  describe '#to_s' do
    let(:dataset) { MockDataset.new([1, 2, 3]) }

    it 'delegates to the wrapped dataset' do
      @query.to_s.must_equal dataset.to_s
    end
  end

  describe '#empty?' do
    describe "when it's empty" do
      it 'returns true' do
        @query.must_be_empty
      end
    end

    describe "when it's filled with elements" do
      let(:dataset) { MockDataset.new([1, 2, 3]) }

      it 'returns false' do
        @query.wont_be_empty
      end
    end
  end

  describe '#any?' do
    describe "when it's empty" do
      it 'returns false' do
        assert !@query.any?
      end
    end

    describe "when it's filled with elements" do
      let(:dataset) { MockDataset.new([1, 2, 3]) }

      it 'returns true' do
        assert @query.any?
      end

      describe "when a block is passed" do
        describe "and it doesn't match elements" do
          it 'returns false' do
            assert !@query.any? {|e| e > 100 }
          end
        end

        describe "and it matches elements" do
          it 'returns true' do
            assert @query.any? {|e| e % 2 == 0 }
          end
        end
      end
    end
  end
end
