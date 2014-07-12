require 'test_helper'

describe Lotus::Model::Adapters::DynamodbAdapter do
  before do
    TestUser = Struct.new(:id, :name, :age) do
      include Lotus::Entity
    end

    TestDevice = Struct.new(:id, :created_at) do
      include Lotus::Entity
    end

    TestPurchase = Struct.new(:id, :region, :subtotal, :item_ids, :content, :created_at, :updated_at) do
      include Lotus::Entity
    end

    coercer = Lotus::Model::Adapters::Dynamodb::Coercer
    @mapper = Lotus::Model::Mapper.new(coercer) do
      collection :test_users do
        entity TestUser

        attribute :id,   String
        attribute :name, String
        attribute :age,  Integer
      end

      collection :test_devices do
        entity TestDevice

        attribute :id,         String, as: :uuid
        attribute :created_at, Time

        identity :uuid
      end

      collection :test_purchases do
        entity TestPurchase

        attribute :id,         String, as: :uuid
        attribute :region,     String
        attribute :subtotal,   Float
        attribute :item_ids,   Set
        attribute :content,    AWS::DynamoDB::Binary
        attribute :created_at, Time
        attribute :updated_at, Time

        identity :uuid
      end
    end.load!

    @adapter = Lotus::Model::Adapters::DynamodbAdapter.new(@mapper)
    @adapter.clear(collection)
  end

  after do
    Object.send(:remove_const, :TestUser)
    Object.send(:remove_const, :TestDevice)
    Object.send(:remove_const, :TestPurchase)
  end

  let(:collection) { :test_users }

  describe 'multiple collections' do
    it 'create records' do
      user   = TestUser.new
      device = TestDevice.new(created_at: Time.new)

      @adapter.clear(:test_users)
      @adapter.clear(:test_devices)

      @adapter.create(:test_users, user)
      @adapter.create(:test_devices, device)

      @adapter.all(:test_users).must_equal   [user]
      @adapter.all(:test_devices).must_equal [device]
    end
  end

  describe '#first' do
    it 'raises an error' do
      -> { @adapter.first(collection) }.must_raise NotImplementedError
    end
  end

  describe '#last' do
    it 'raises an error' do
      -> { @adapter.last(collection) }.must_raise NotImplementedError
    end
  end

  describe '#persist' do
    describe 'when the given entity is not persisted' do
      let(:entity) { TestUser.new }

      it 'stores the record and assigns an id' do
        @adapter.persist(collection, entity)

        entity.id.wont_be_nil
        @adapter.find(collection, entity.id).must_equal entity
      end
    end

    describe 'when the given entity is persisted' do
      before do
        @adapter.create(collection, entity)
      end

      let(:entity) { TestUser.new }

      it 'updates the record and leaves untouched the id' do
        id = entity.id
        id.wont_be_nil

        entity.name = 'L'
        @adapter.persist(collection, entity)

        entity.id.must_equal(id)
        @adapter.find(collection, entity.id).must_equal entity
      end
    end
  end

  describe '#create' do
    let(:entity) { TestUser.new }

    it 'stores the record and assigns an id' do
      @adapter.create(collection, entity)

      entity.id.wont_be_nil
      @adapter.find(collection, entity.id).must_equal entity
    end

    describe 'preserves nil attributes' do
      let(:collection) { :test_purchases }
      let(:entity) { TestPurchase.new(region: "europe", created_at: Time.new) }

      it do
        entity.updated_at.must_equal nil
        @adapter.create(collection, entity)

        found_entity = @adapter.find(collection, "europe", entity.created_at)
        found_entity.updated_at.must_be_nil
      end
    end
  end

  describe '#update' do
    before do
      @adapter.create(collection, entity)
    end

    let(:entity) { TestUser.new(id: nil, name: 'L') }

    it 'stores the changes and leave the id untouched' do
      id = entity.id

      entity.name = 'MG'
      @adapter.update(collection, entity)

      entity.id.must_equal id
      @adapter.find(collection, entity.id).must_equal entity
    end

    it 'removes attribute' do
      entity.name = nil
      @adapter.update(collection, entity)

      found_entity = @adapter.find(collection, entity.id)
      found_entity.must_equal entity
      found_entity.name.must_be_nil
    end
  end

  describe '#delete' do
    before do
      @adapter.create(collection, entity)
    end

    let(:entity) { TestUser.new }

    it 'removes the given identity' do
      @adapter.delete(collection, entity)
      @adapter.find(collection, entity.id).must_be_nil
    end
  end

  describe '#all' do
    describe 'when no records are persisted' do
      it 'returns an empty collection' do
        @adapter.all(collection).must_be_empty
      end
    end

    describe 'when some records are persisted' do
      before do
        @adapter.create(collection, entity)
      end

      let(:entity) { TestUser.new }

      it 'returns all of them' do
        @adapter.all(collection).must_equal [entity]
      end
    end

    describe 'when large records set is persisted' do
      before do
        entities.each do |entity|
          @adapter.create(collection, entity)
        end
      end

      let(:entities) { 25.times.map { TestUser.new(name: 'A'*50_000) } }

      it 'returns all of them' do
        @adapter.all(collection).count.must_equal entities.count
      end
    end
  end

  describe '#each' do
    describe 'when no records are persisted' do
      it 'returns an empty collection' do
        query = Proc.new {}
        counter = 0
        @adapter.query(collection, &query).each { |x| counter += 1 }
        counter.must_equal 0
      end
    end

    describe 'when some records are persisted' do
      before do
        @adapter.create(collection, entity)
      end

      let(:entity) { TestUser.new }

      it 'returns all of them' do
        query = Proc.new {}
        counter = 0
        @adapter.query(collection, &query).each { |x| counter += 1 }
        counter.must_equal 1
      end
    end

    describe 'when large records set is persisted' do
      before do
        entities.each do |entity|
          @adapter.create(collection, entity)
        end
      end

      let(:entities) { 25.times.map { TestUser.new(name: 'A'*50_000) } }

      it 'returns all of them' do
        query = Proc.new {}
        counter = 0
        count = @adapter.query(collection, &query).each { |x| counter += 1 }
        counter.must_equal entities.count
        count.must_equal entities.count
      end
    end
  end

  describe '#find' do
    before do
      @adapter.create(collection, entity)
    end

    describe 'simple key' do
      let(:entity) { TestUser.new }

      it 'returns the record by id' do
        @adapter.find(collection, entity.id).must_equal entity
      end

      it 'returns nil when the record cannot be found' do
        @adapter.find(collection, 1_000_000.to_s).must_be_nil
      end

      it 'returns nil when the given id is nil' do
        @adapter.find(collection, nil).must_be_nil
      end

      it 'returns nil when the given id is empty string' do
        @adapter.find(collection, nil).must_be_nil
      end
    end

    describe 'complex key' do
      let(:entity) { TestDevice.new(created_at: Time.new) }
      let(:collection) { :test_devices }

      it 'returns the record by id' do
        @adapter.find(collection, entity.id, entity.created_at).must_equal entity
      end

      it 'returns nil when not enough keys' do
        @adapter.find(collection, entity.id).must_be_nil
      end
    end
  end

  describe '#clear' do
    before do
      @adapter.create(collection, entity)
    end

    let(:entity) { TestUser.new }

    it 'removes all the records' do
      @adapter.clear(collection)
      @adapter.all(collection).must_be_empty
    end
  end

  describe '#query' do
    let(:collection) { :test_purchases }
    let(:purchase1) do
      TestPurchase.new(
        region: 'europe',
        subtotal: 15.0,
        item_ids: [1, 2, 3],
        content: "OMG",
        created_at: Time.new,
      )
    end
    let(:purchase2) do
      TestPurchase.new(
        region: 'europe',
        subtotal: 10.0,
        item_ids: ["2", "3", "4"],
        content: AWS::DynamoDB::Binary.new("SO"),
        created_at: Time.new,
      )
    end
    let(:purchase3) do
      TestPurchase.new(
        region: 'usa',
        subtotal: 5.0,
        item_ids: [AWS::DynamoDB::Binary.new("WOW")],
        content: AWS::DynamoDB::Binary.new("MUCH"),
        created_at: Time.new,
      )
    end
    let(:purchase4) do
      TestPurchase.new(
        region: 'asia',
        subtotal: 100.0,
        item_ids: [4, 5, 6],
        content: AWS::DynamoDB::Binary.new("CONTENT"),
        created_at: Time.new,
      )
    end
    let(:purchase5) do
      TestPurchase.new(
        region: 'europe',
        subtotal: 1.0,
        created_at: Time.new,
      )
    end
    let(:purchases) { [purchase1, purchase2, purchase3, purchase4] }

    describe 'types' do
      before do
        purchases.each do |purchase|
          @adapter.create(collection, purchase)
        end

        @purchases = @adapter.query(collection).all.sort_by(&:created_at)
      end

      it 'has string type' do
        @purchases.first.region.class.must_equal String
      end

      it 'has number type' do
        @purchases.first.subtotal.class.must_equal Float
      end

      it 'has set type' do
        @purchases.first.item_ids.class.must_equal Set
        @purchases.at(0).item_ids.map(&:class).must_equal [BigDecimal, BigDecimal, BigDecimal]
        @purchases.at(1).item_ids.map(&:class).must_equal [String, String, String]
        @purchases.at(2).item_ids.map(&:class).must_equal [AWS::DynamoDB::Binary]
      end

      it 'has binary type' do
        @purchases.each do |purchase|
          purchase.content.class.must_equal AWS::DynamoDB::Binary
        end
      end
    end

    describe 'where' do
      describe 'with an empty collection' do
        it 'returns an empty result set' do
          result = @adapter.query(collection) do
            where(region: 'europe')
          end.all

          result.must_be_empty
        end
      end

      describe 'with a filled collection' do
        before do
          purchases.each do |purchase|
            @adapter.create(collection, purchase)
          end
        end

        describe 'without key schema' do
          it 'returns selected records' do
            query = Proc.new {
              where(subtotal: 100.0)
            }

            result = @adapter.query(collection, &query).all
            result.must_equal [purchase4]
          end
        end

        describe 'with key schema' do
          it 'returns selected records' do
            query = Proc.new {
              where(region: 'europe')
            }

            result = @adapter.query(collection, &query).all
            result.must_equal [purchase1, purchase2]
          end

          it 'can use multiple where conditions' do
            created_at = purchase1.created_at
            query = Proc.new {
              where(region: 'europe').where(created_at: created_at)
            }

            result = @adapter.query(collection, &query).all
            result.must_equal [purchase1]
          end

          it 'can use array as where condition' do
            query = Proc.new {
              where(region: 'europe').where(subtotal: [15.0, 10.0])
            }

            result = @adapter.query(collection, &query).all
            result.must_equal [purchase1, purchase2]
          end

          it 'can use range as where condition' do
            query = Proc.new {
              where(region: 'europe').where(subtotal: 8..14)
            }

            result = @adapter.query(collection, &query).all
            result.must_equal [purchase2]
          end

          it 'can use "eq" alias' do
            query = Proc.new {
              eq(region: 'europe')
            }

            @adapter.query(collection, &query).count.must_equal 2
          end

          it 'can use "in" alias' do
            @adapter.create(collection, purchase5)

            query = Proc.new {
              where(region: 'europe').in(subtotal: [10.0, 1.0])
            }

            @adapter.query(collection, &query).count.must_equal 2
          end

          it 'can use "between" alias' do
            @adapter.create(collection, purchase5)

            query = Proc.new {
              where(region: 'europe').between(subtotal: 0.0..11.0)
            }

            @adapter.query(collection, &query).count.must_equal 2
          end
        end
      end
    end

    describe 'exclude' do
      describe 'with an empty collection' do
        it 'returns an empty result set' do
          result = @adapter.query(collection) do
            exclude(subtotal: 10.0)
          end.all

          result.must_be_empty
        end
      end

      describe 'with a filled collection' do
        before do
          purchases.each do |purchase|
            @adapter.create(collection, purchase)
          end
        end

        describe 'without key schema' do
          it 'returns selected records' do
            query = Proc.new {
              exclude(subtotal: 100.0)
            }

            result = @adapter.query(collection, &query).all
            result.must_include purchase1
            result.must_include purchase2
            result.must_include purchase3
          end
        end

        describe 'with key schema' do
          it 'returns selected records' do
            query = Proc.new {
              where(region: 'europe').exclude(subtotal: 15.0)
            }

            result = @adapter.query(collection, &query).all
            result.must_equal [purchase2]
          end

          it 'can use multiple exclude conditions' do
            id = purchase2.id

            query = Proc.new {
              where(region: 'europe').exclude(subtotal: 15.0).exclude(uuid: id)
            }

            result = @adapter.query(collection, &query).all
            result.must_equal []
          end

          it 'can use multiple exclude conditions with "not" alias' do
            id = purchase2.id

            query = Proc.new {
              where(region: 'europe').not(subtotal: 15.0).not(uuid: id)
            }

            result = @adapter.query(collection, &query).all
            result.must_equal []
          end

          it 'can not use array as exclude condition' do
            query = Proc.new {
              exclude(subtotal: [15.0, 10.0])
            }

            ->{ @adapter.query(collection, &query).all }.must_raise \
              NotImplementedError
          end

          it 'can use "ne" alias' do
            query = Proc.new {
              where(region: 'europe').ne(subtotal: 1.0)
            }

            @adapter.query(collection, &query).count.must_equal 2
          end
        end
      end
    end

    describe 'comparison' do
      describe 'with an empty collection' do
        it 'returns an empty result set' do
          result = @adapter.query(collection) do
            le(subtotal: 10.0)
          end.all

          result.must_be_empty
        end
      end

      describe 'with a filled collection' do
        before do
          purchases.each do |purchase|
            @adapter.create(collection, purchase)
          end
        end

        it 'can use "le" method' do
          query = Proc.new {
            where(region: 'europe').le(subtotal: 15.0)
          }

          @adapter.query(collection, &query).count.must_equal 2
        end

        it 'can use "lt" method' do
          query = Proc.new {
            where(region: 'europe').lt(subtotal: 15.0)
          }

          @adapter.query(collection, &query).count.must_equal 1
        end

        it 'can use "ge" method' do
          query = Proc.new {
            where(region: 'europe').ge(subtotal: 10.0)
          }

          @adapter.query(collection, &query).count.must_equal 2
        end

        it 'can use "gt" method' do
          query = Proc.new {
            where(region: 'europe').gt(subtotal: 10.0)
          }

          @adapter.query(collection, &query).count.must_equal 1
        end

        it 'can use "contains" method' do
          query = Proc.new {
            where(region: 'europe').contains(item_ids: 2)
          }

          @adapter.query(collection, &query).count.must_equal 1
        end

        it 'can use "not_contains" method' do
          skip_for_fake_dynamo
          query = Proc.new {
            where(region: 'europe').not_contains(item_ids: '2')
          }

          @adapter.query(collection, &query).count.must_equal 1
        end

        it 'can use "begins_with" method' do
          query = Proc.new {
            where(region: 'asia').begins_with(content: "CON")
          }

          @adapter.query(collection, &query).count.must_equal 1
        end

        it 'can use "null" method' do
          skip_for_fake_dynamo
          @adapter.create(collection, purchase5)

          query = Proc.new {
            where(region: 'europe').null(:content)
          }

          @adapter.query(collection, &query).count.must_equal 1
        end

        it 'can use "not_null" method' do
          skip_for_fake_dynamo
          @adapter.create(collection, purchase5)

          query = Proc.new {
            where(region: 'europe').not_null(:content)
          }

          @adapter.query(collection, &query).count.must_equal 2
        end
      end
    end

    describe 'or' do
      describe 'with an empty collection' do
        it 'returns an empty result set' do
          result = @adapter.query(collection) do
            where(subtotal: 10.0).or.where(uuid: "omg")
          end.all

          result.must_be_empty
        end
      end

      describe 'with a filled collection' do
        before do
          purchases.each do |purchase|
            @adapter.create(collection, purchase)
          end
        end

        it 'returns selected records' do
          skip_for_fake_dynamo
          id = purchase1.id

          query = Proc.new {
            where(region: 'europe').where(subtotal: 10.0).or.where(uuid: id)
          }

          result = @adapter.query(collection, &query).all
          result.must_equal [purchase1, purchase2]
        end
      end
    end

    describe 'select' do
      describe 'with an empty collection' do
        it 'returns an empty result' do
          result = @adapter.query(collection) do
            select(:subtotal)
          end.all

          result.must_be_empty
        end
      end

      describe 'with a filled collection' do
        before do
          purchases.each do |purchase|
            @adapter.create(collection, purchase)
          end
        end

        it 'returns the selected columns from all the records' do
          query = Proc.new {
            select(:subtotal)
          }

          result = @adapter.query(collection, &query).all
          purchases.each do |purchase|
            record = result.find { |r| r.subtotal == purchase.subtotal }
            record.wont_be_nil
            record.region.must_be_nil
          end
        end

        it 'returns only the select of requested records' do
          query = Proc.new {
            where(region: 'europe').select(:subtotal)
          }

          result = @adapter.query(collection, &query).all

          record = result.first
          record.subtotal.must_equal(purchase1.subtotal)
          record.region.must_be_nil
        end

        it 'returns only the multiple select of requested records' do
          query = Proc.new {
            where(region: 'europe').select(:subtotal, :region)
          }

          result = @adapter.query(collection, &query).all

          record = result.first
          record.subtotal.must_equal(purchase1.subtotal)
          record.region.must_equal(purchase1.region)
          record.id.must_be_nil
        end
      end
    end

    describe 'order' do
      let(:collection) { :test_devices }
      let(:device1) { TestDevice.new(id: 'device', created_at: Time.new) }
      let(:device2) { TestDevice.new(id: 'device', created_at: Time.new) }
      let(:devices) { [device1, device2] }

      describe 'asc' do
        describe 'with an empty collection' do
          it 'returns an empty result set' do
            result = @adapter.query(collection) do
              where(uuid: 'device').asc
            end.all

            result.must_be_empty
          end
        end

        describe 'with a filled collection' do
          before do
            devices.each do |device|
              @adapter.create(collection, device)
            end
          end

          it 'returns sorted records' do
            query = Proc.new {
              where(uuid: 'device').asc
            }

            result = @adapter.query(collection, &query).all
            result.must_equal [device1, device2]
          end
        end
      end

      describe 'desc' do
        describe 'with an empty collection' do
          it 'returns an empty result set' do
            result = @adapter.query(collection) do
              where(uuid: 'device').desc
            end.all

            result.must_be_empty
          end
        end

        describe 'with a filled collection' do
          before do
            devices.each do |device|
              @adapter.create(collection, device)
            end
          end

          it 'returns reverse sorted records' do
            query = Proc.new {
              where(uuid: 'device').desc
            }

            result = @adapter.query(collection, &query).all
            result.must_equal [device1, device2]
          end
        end
      end
    end

    describe 'limit' do
      describe 'with an empty collection' do
        it 'returns an empty result set' do
          result = @adapter.query(collection) do
            limit(1)
          end.all

          result.must_be_empty
        end
      end

      describe 'with a filled collection' do
        before do
          purchases.each do |purchase|
            @adapter.create(collection, purchase)
          end
        end

        it 'returns only the number of requested records' do
          query = Proc.new {
            where(region: 'europe').limit(1)
          }

          result = @adapter.query(collection, &query).all
          result.must_equal [purchase1]
        end
      end

      describe 'with large records set' do
        before do
          purchases.each do |entity|
            @adapter.create(collection, entity)
          end
        end

        let(:purchases) do
          25.times.map do |i|
            TestPurchase.new(
              region: 'europe',
              content: ('A'..'Z').to_a[i]*50_000,
              created_at: Time.new,
            )
          end
        end

        it 'returns all of them' do
          query = Proc.new {
            where(region: 'europe').limit(24)
          }

          result = @adapter.query(collection, &query).count
          result.must_equal 24
        end
      end
    end

    describe 'exists?' do
      describe 'with an empty collection' do
        it 'returns false' do
          result = @adapter.query(collection) do
            where(region: 'wow')
          end.exists?

          result.must_equal false
        end
      end

      describe 'with a filled collection' do
        before do
          purchases.each do |purchase|
            @adapter.create(collection, purchase)
          end
        end

        it 'returns true when there are matched records' do
          query = Proc.new {
            where(region: 'asia')
          }

          result = @adapter.query(collection, &query).exists?
          result.must_equal true
        end

        it 'returns false when there are matched records' do
          query = Proc.new {
            where(region: 'wtf')
          }

          result = @adapter.query(collection, &query).exists?
          result.must_equal false
        end

        it 'can use "exist?" alias' do
          query = Proc.new {
            where(region: 'usa')
          }

          result = @adapter.query(collection, &query).exist?
          result.must_equal true
        end
      end
    end

    describe 'count' do
      describe 'with an empty collection' do
        it 'returns 0' do
          result = @adapter.query(collection) do
            all
          end.count

          result.must_equal 0
        end
      end

      describe 'with a filled collection' do
        before do
          purchases.each do |purchase|
            @adapter.create(collection, purchase)
          end
        end

        it 'returns the count of all the records' do
          query = Proc.new {
            all
          }

          result = @adapter.query(collection, &query).count
          result.must_equal 4
        end

        it 'returns the count from an empty query block' do
          query = Proc.new {
          }

          result = @adapter.query(collection, &query).count
          result.must_equal 4
        end

        it 'returns only the count of requested records' do
          query = Proc.new {
            where(region: 'europe')
          }

          result = @adapter.query(collection, &query).count
          result.must_equal 2
        end
      end

      describe 'with large records set' do
        before do
          purchases.each do |entity|
            @adapter.create(collection, entity)
          end
        end

        let(:purchases) do
          25.times.map do |i|
            TestPurchase.new(
              region: 'europe',
              content: ('A'..'Z').to_a[i]*50_000,
              created_at: Time.new,
            )
          end
        end

        it 'returns all of them' do
          query = Proc.new {
            where(region: 'europe')
          }

          result = @adapter.query(collection, &query).count
          result.must_equal purchases.count
        end
      end
    end

    describe 'consistent' do
      before do
        purchases.each do |purchase|
          @adapter.create(collection, purchase)
        end
      end

      it 'does not fail' do
        query = Proc.new {
          where(region: 'europe').consistent
        }

        result = @adapter.query(collection, &query).count
        result.must_equal 2
      end
    end

    describe 'index' do
      describe 'with an empty collection' do
        it 'returns an empty result for local index' do
          result = @adapter.query(collection) do
            index('by_subtotal').where(region: 'europe')
          end.all

          result.must_be_empty
        end

        it 'returns an empty result for global index' do
          result = @adapter.query(collection) do
            index('by_uuid').where(uuid: 'wow')
          end.all

          result.must_be_empty
        end
      end

      describe 'with a filled collection' do
        before do
          purchases.each do |purchase|
            @adapter.create(collection, purchase)
          end
        end

        describe 'local index' do
          it 'returns selected records' do
            query = Proc.new {
              index('by_subtotal').where(region: 'europe')
            }

            result = @adapter.query(collection, &query).all
            result.must_equal [purchase2, purchase1]
          end
        end

        describe 'global index' do
          it 'returns selected records' do
            id = purchase3.id

            query = Proc.new {
              index('by_uuid').where(uuid: id)
            }

            result = @adapter.query(collection, &query).all
            result.must_equal [purchase3]
          end
        end
      end
    end
  end
end
