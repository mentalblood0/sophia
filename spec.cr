require "log"
require "wait_group"
require "spec"

require "./src/sophia"

Log.setup :debug

describe Sophia do
  describe "Database" do
    env = Sophia::Environment.new({
      "sophia.path"              => "/tmp/sophia",
      "db"                       => "test",
      "db.test.compression"      => "zstd",
      "db.test.compaction.cache" => 4_i64 * 1024 * 1024 * 1024,
    }.merge Sophia.scheme_conf("test", {"a" => String}, {"b" => String}))

    db = Sophia::Database({a: String}, {b: String}).new env, "test"
    a = Random::DEFAULT.hex 8
    b = Random::DEFAULT.hex 8

    it "sets key=value" do
      db[{a: a}] = {b: b}
    end

    it "check if has key" do
      db.has_key?({a: a})
    end

    it "get value by key" do
      db[{a: a}]?.should eq({b: b})
    end

    it "iterate from key" do
      db.from({a: a}, ">=") do |k, v|
        db[k]?.should eq(v)
      end
    end

    it "delete key/value pair" do
      db.delete({a: a})
      db[{a: a}]?.should eq nil
    end

    it "do all the same in transaction" do
      env.transaction do |tr|
        dbtr = db.in tr
        db[{a: a}] = {b: b}
        db[{a: a}]?.should eq({b: b})
        db.delete({a: a})
        db[{a: a}]?.should eq nil
      end
    end
  end
end
