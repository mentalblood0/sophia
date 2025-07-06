require "log"
require "wait_group"
require "spec"

require "./src/sophia"

Log.setup :debug

describe Sophia do
  env = Sophia::Environment.new({
    "sophia.path"              => "/tmp/sophia",
    "db"                       => "test",
    "db.test.compaction.cache" => 4_i64 * 1024 * 1024 * 1024,
    "db.test.scheme"           => ["a", "b"],
    "db.test.scheme.a"         => "string,key(0)",
    "db.test.scheme.b"         => "string",
  })
  describe "Environment" do
    it "conf" do
      env.getstring("sophia.path").should eq "/tmp/sophia"
      env.getint("db.test.compaction.cache").should eq 4_i64 * 1024 * 1024 * 1024
    end
  end
  describe "Database" do
    db = Sophia::Database({a: String}, {b: String}).new env, "test"
    a = Random::DEFAULT.hex 8
    b = Random::DEFAULT.hex 8
    it "CRUD" do
      # set key=value
      db[{a: a}] = {b: b}

      # get value by key
      db[{a: a}]?.should eq({b: b})

      # iterate from key
      db.from({a: a}, ">=") do |k, v|
        db[k]?.should eq(v)
      end

      # delete key/value pair
      db.delete({a: a})
      db[{a: a}]?.should eq nil

      # # delete key/value pair
      # env.transaction do |tr|
      #   tr.delete db.document({"key" => key}) # lowlevel, in transactcion
      #   tr.delete db, key                     # alias, in transaction
      # end
      # db.delete db.document({"key" => key}) # lowlevel, out of transaction
      # db.delete key                         # alias, out of transactcion
      # db[key]?.should eq nil
    end
  end
end
