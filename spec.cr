require "wait_group"
require "spec"

require "./src/sophia"

Log.setup :debug

describe Sophia do
  describe "Database" do
    db = Sophia::Database({id: UInt32}, {url: String, state: UInt8, tags: String}).new "test", Sophia::H{"compression"      => "zstd",
                                                                                                         "compaction.cache" => 4_i64 * 1024 * 1024 * 1024}
    env = Sophia::Environment.new Sophia::H{"sophia.path" => "/tmp/sophia"}, db

    a = Random::DEFAULT.hex 8
    b = Random::DEFAULT.hex 8
    key = {id: 1_u32}
    value = {url: "url", state: 0_u8, tags: ""}

    it "sets key=value" do
      db[key] = value
    end

    it "check if has key" do
      db.has_key?(key).should eq true
    end

    it "get value by key" do
      db[key]?.should eq value
    end

    it "iterate from key" do
      db.from(key, ">=") do |k, v|
        db[k]?.should eq v
      end
    end

    it "delete key/value pair" do
      db.delete key
      db[key]?.should eq nil
    end

    it "do all the same in transaction" do
      env.transaction do |tr|
        dbtr = db.in tr
        db[key] = value
        db[key]?.should eq value
        db.delete key
        db[key]?.should eq nil
      end
    end
  end
end
