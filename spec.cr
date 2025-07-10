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
    td = {name: "tag", type: "type"}

    pk = {_0_host: "host", _1_id: 1_u32}
    pv = {url: "url", tags: "tags", value_state: ValueState::B}
    pd = {_0_host: "host", _1_id: 1_u32, url: "url", tags: "tags", value_state: ValueState::B}

    sk = {_0_state: State::Rejected, _1_post_id: 4_u32}
    sd = sk

    it "inserts documents" do
      env << td << pd << sd
    end

    it "check if has key" do
      env.has_key?(tk).should eq true
      env.has_key?(pk).should eq true
      env.has_key?(sk).should eq true
    end

    it "get value by key" do
      env[tk]?.should eq tv
      env[pk]?.should eq pv
    end

    it "iterate from key" do
      env.from(tk, ">=") do |k, v|
        env[k]?.should eq v
      end
    end

    it "delete key/value pair" do
      env.delete tk
      env[tk]?.should eq nil
      env.delete pk
      env[pk]?.should eq nil

      env.has_key?(sk).should eq true
      env.delete sk
      env.has_key?(sk).should eq false
    end

    it "perform operations in transaction" do
      env.transaction do |tx|
        tx << td
        tx[tk]?.should eq tv
        tx.delete tk
        tx[tk]?.should eq nil
      end
    end
  end
end
