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
      env.transaction do |tr|
        tr << db.document({"key" => key, "value" => value})
      end
      env.transaction do |tr|
        tr[db.document({"key" => key})]?.not_nil!["value"]?.not_nil!.should eq value
      end
      db[db.document({"key" => key})]?.not_nil!["value"]?.not_nil!.should eq value
      db[key]?.should eq value
      db.delete key
      db[key]?.should eq nil
    end
  end
end
