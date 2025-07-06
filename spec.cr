require "wait_group"
require "spec"

require "./src/sophia"

Log.setup :debug

describe Sophia do
  env = Sophia::Environment.new({
    "sophia.path"              => "/tmp/sophia",
    "db"                       => "test",
    "db.test.compaction.cache" => 4_i64 * 1024 * 1024 * 1024,
  })
  describe "Environment" do
    it "conf" do
      env.getstring("sophia.path").should eq "/tmp/sophia"
      env.getint("db.test.compaction.cache").should eq 4_i64 * 1024 * 1024 * 1024
    end
  end
  describe "Database" do
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
        tr[db, key]?.not_nil!["value"]?.should eq value                     # alias, in transaction
        tr[db, key].should eq value                                         # alias, in transaction
      end
      db[db.document({"key" => key})]?.not_nil!["value"]?.should eq value # lowlevel, out of transaction
      db[key]?.not_nil!["value"]?.should eq value                         # alias, out of transaction
      db[key].should eq value                                             # alias, out of transaction

      db[key + "2"] = value
      db[key + "1"] = ""
      db[key + "0"] = nil
      db[key + "1"]?.should_not eq nil
      db[key + "0"]?.should_not eq nil
      db[key + "1"].should eq nil
      db[key + "0"].should eq nil
      db[key, key + "0", key + "1", key + "2"].should eq({key       => value,
                                                          key + "0" => nil,
                                                          key + "1" => nil,
                                                          key + "2" => value})

      # iterate from key
      env.from db, key, ">=" do |key, value|
        Log.debug { "#{key} = #{value}" }
      end

      # delete key/value pair
      env.transaction do |tr|
        tr.delete db.document({"key" => key}) # lowlevel, in transactcion
        tr.delete db, key                     # alias, in transaction
      end
      db.delete db.document({"key" => key}) # lowlevel, out of transaction
      db.delete key                         # alias, out of transactcion
      db[key]?.should eq nil
    end
  end
end
