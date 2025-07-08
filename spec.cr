require "wait_group"
require "spec"

require "./src/sophia"

Log.setup :debug

describe Sophia do
  describe "Database" do
    opts = Sophia::H{"compression"      => "zstd",
                     "compaction.cache" => 1_i64 * 1024 * 1024 * 1024}
    tags = Sophia::Database({tag: String}, {type: String}).new "tag", opts
    posts = Sophia::Database({id: UInt32}, {url: String, state: UInt8, tags: String}).new "post", opts
    posts_by_state = Sophia::Database({_0_state: UInt8, _1_id: UInt32}, {value: String}).new "post_by_state", opts

    env = Sophia::Environment.new Sophia::H{"sophia.path" => "/tmp/sophia"}, tags, posts, posts_by_state

    a = Random::DEFAULT.hex 8
    b = Random::DEFAULT.hex 8

    it "sets key=value" do
      tags[{tag: "tag"}] = {type: "type"}
      posts[{id: 1_u32}] = {url: "url", state: 2_u8, tags: "tags"}
    end

    it "check if has key" do
      tags.has_key?({tag: "tag"}).should eq true
    end

    it "get value by key" do
      tags[{tag: "tag"}]?.should eq({type: "type"})
      posts[{id: 1_u32}]?.should eq({url: "url", state: 2_u8, tags: "tags"})
    end

    it "iterate from key" do
      tags.from({tag: "tag"}, ">=") do |k, v|
        tags[k]?.should eq v
      end
    end

    it "delete key/value pair" do
      tags.delete({tag: "tag"})
      tags[{tag: "tag"}]?.should eq nil
    end

    it "do all the same in transaction" do
      env.transaction do |tr|
        tagstr = tags.in tr
        tags[{tag: "tag"}] = {type: "type"}
        tags[{tag: "tag"}]?.should eq({type: "type"})
        tags.delete({tag: "tag"})
        tags[{tag: "tag"}]?.should eq nil
      end
    end
  end
end
