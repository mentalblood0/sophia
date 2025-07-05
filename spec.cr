require "wait_group"
require "spec"

require "./src/sophia"

Log.setup :debug

describe Sophia do
  env = Sophia::Environment.new({"sophia.path" => "/tmp/sophia", "db" => "test", "db.test.compaction.cache" => 4_i64 * 1024 * 1024 * 1024})
  describe "Db" do
    db = env.database?("test").not_nil!
    key = Random::DEFAULT.hex 8
    value = Random::DEFAULT.hex 8
    it "CRUD" do
      # set key=value
      env.transaction do |tr|
        tr << db.document({"key" => key, "value" => value}) # lowlevel, in transaction
        tr[db, key] = value                                 # alias, in transaction
      end
      db << db.document({"key" => key, "value" => value}) # lowlevel, out of transaction
      db[key] = value                                     # alias, out of transaction

      # get value by key
      env.transaction do |tr|
        tr[db.document({"key" => key})]?.not_nil!["value"]?.should eq value # lowlevel, in transaction
        tr[db, key]?.should eq value                                        # alias, in transaction
      end
      db[db.document({"key" => key})]?.not_nil!["value"]?.should eq value # lowlevel, out of transaction
      db[key]?.should eq value                                            # alias, out of transaction

      # delete key/value pair
      env.transaction do |tr|
        tr.delete db.document({"key" => key}) # lowlevel, in transactcion
        tr.delete db, key                     # alias, in transaction
      end
      db.delete db.document({"key" => key}) # lowlevel, out of transaction
      db.delete key                         # alias, out of transactcion
      db[key]?.should eq nil
    end
    it "works asynchronously" do
      Log.setup :info
      value2 = Random::DEFAULT.hex 8
      end_time = Time.utc + 2.seconds
      spawn { loop do
        db[key] = value
        break if Time.utc >= end_time
      end }
      spawn { loop do
        db[value] = key
        break if Time.utc >= end_time
      end }
      spawn { loop do
        db[key] = value2
        break if Time.utc >= end_time
      end }
      spawn { loop do
        db[value2] = key
        break if Time.utc >= end_time
      end }
      spawn { loop do
        db.delete key
        break if Time.utc >= end_time
      end }
      spawn { loop do
        db.delete value
        break if Time.utc >= end_time
      end }
      spawn { loop do
        db.delete value2
        break if Time.utc >= end_time
      end }
    end
  end
end
