require "wait_group"
require "spec"

require "./src/sophia"

Log.setup :debug

Sophia.define_env TestEnv,
  {tags: {key: {tag: String},
          value: {type: String},

  }, posts: {key: {id: UInt32},
             value: {url: String,
                     state: UInt8,
                     tags: String},

  }, posts_by_state: {key: {_0_state: UInt8,
                            _1_id: UInt32},
                      value: {value: String}}}

describe Sophia do
  describe "exp" do
  end

  describe "Database" do
    opts = Sophia::H{"compression"      => "zstd",
                     "compaction.cache" => 1_i64 * 1024 * 1024 * 1024}
    env = TestEnv.new Sophia::H{"sophia.path" => "/tmp/sophia"}, {tags: opts, posts: opts, posts_by_state: opts}

    a = Random::DEFAULT.hex 8
    b = Random::DEFAULT.hex 8

    it "sets key=value" do
      env.tags[{tag: "tag"}] = {type: "type"}
      env.posts[{id: 1_u32}] = {url: "url", state: 2_u8, tags: "tags"}
    end

    it "check if has key" do
      env.tags.has_key?({tag: "tag"}).should eq true
    end

    it "get value by key" do
      env.tags[{tag: "tag"}]?.should eq({type: "type"})
      env.posts[{id: 1_u32}]?.should eq({url: "url", state: 2_u8, tags: "tags"})
    end

    it "iterate from key" do
      env.tags.from({tag: "tag"}, ">=") do |k, v|
        env.tags[k]?.should eq v
      end
    end

    it "delete key/value pair" do
      env.tags.delete({tag: "tag"})
      env.tags[{tag: "tag"}]?.should eq nil
    end

    it "do all the same in transaction" do
      env.transaction do |tx|
        ttags = env.tags.dup
        ttags.tx = tx
        ttags[{tag: "tag"}] = {type: "type"}
        ttags[{tag: "tag"}]?.should eq({type: "type"})
        ttags.delete({tag: "tag"})
        ttags[{tag: "tag"}]?.should eq nil
      end
    end
  end
end
