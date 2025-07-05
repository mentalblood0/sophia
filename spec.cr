require "spec"

require "./src/sophia"

Log.setup :debug

describe Sophia do
  env = Sophia::Environment.new({"sophia.path" => "/tmp/sophia", "db" => "test", "db.test.compaction.cache" => 4_i64 * 1024 * 1024 * 1024})
  describe "Db" do
    db = env.database?("test").not_nil!
    it "CRUD" do
      key = Random::DEFAULT.hex 8
      value = Random::DEFAULT.hex 8

      # set key=value
      env.transaction do |tr|
        tr << db.document({"key" => key, "value" => value}) # lowlevel, in transaction
        tr[db, key] = value                                 # just key/value, in transaction
      end
      db << db.document({"key" => key, "value" => value}) # lowlevel, out of transaction
      db[key] = value                                     # just key/value, out of transaction

      # get value by key
      env.transaction do |tr|
        tr[db.document({"key" => key})]?.not_nil!["value"]?.should eq value # lowlevel, in transaction
        tr[db, key]?.should eq value                                        # just key, in transaction
      end
      db[db.document({"key" => key})]?.not_nil!["value"]?.should eq value # lowlevel, out of transaction
      db[key]?.should eq value                                            # just key, out of transaction

      # delete key/value pair
      env.transaction do |tr|
        tr.delete db.document({"key" => key}) # lowlevel, in transactcion
        tr.delete db, key                     # just key, in transaction
      end
      db.delete db.document({"key" => key}) # lowlevel, out of transaction
      db.delete key                         # just key, out of transactcion
      db[key]?.should eq nil
    end
  end
end
