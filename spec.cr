require "wait_group"
require "spec"

require "./src/sophia"

Log.setup :debug

enum State : UInt8
  Imported = 0
  Rejected = 1
  Missing  = 2
end

enum ValueState : UInt8
  A = 0
  B = 1
  C = 2
end

Sophia.define_env TestEnv, {tags: {key: {name: String},
                                   value: {type: String}},
                            posts: {key: {_0_host: String,
                                          _1_id: UInt32},
                                    value: {url: String,
                                            tags: String,
                                            value_state: ValueState}},
                            states: {key: {_0_state: State,
                                           _1_post_id: UInt32}}}

describe Sophia do
  describe "exp" do
  end

  describe "Database" do
    opts = Sophia::H{"compression"      => "zstd",
                     "compaction.cache" => 1_i64 * 1024 * 1024 * 1024}
    env = TestEnv.new Sophia::H{"sophia.path" => "/tmp/sophia"}, {tags: opts, posts: opts, states: opts}

    tk = {name: "tag"}
    tv = {type: "type"}
    pk = {_0_host: "host", _1_id: 1_u32}
    pv = {url: "url", tags: "tags", value_state: ValueState::B}
    sk = {_0_state: State::Rejected, _1_post_id: 4_u32}

    it "sets key=value" do
      env.tags[tk] = tv
      env.posts[pk] = pv
      env.states[sk] = nil
    end

    it "check if has key" do
      env.tags.has_key?(tk).should eq true
      env.posts.has_key?(pk).should eq true
    end

    it "get value by key" do
      env.tags[tk]?.should eq tv
      env.posts[pk]?.should eq pv
    end

    it "iterate from key" do
      env.tags.from(tk, ">=") do |k, v|
        env.tags[k]?.should eq v
      end
    end

    it "delete key/value pair" do
      env.tags.delete tk
      env.tags[tk]?.should eq nil
      env.posts.delete pk
      env.posts[pk]?.should eq nil
    end

    it "do all the same in transaction" do
      env.transaction do |tx|
        ttags = env.tags.dup
        ttags.tx = tx
        ttags[tk] = tv
        ttags[tk]?.should eq tv
        ttags.delete tk
        ttags[tk]?.should eq nil
      end
    end
  end
end
