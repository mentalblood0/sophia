require "spec"

require "../src/sophia"

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
                            posts: {key: {host: String,
                                          id: UInt32},
                                    value: {url: String,
                                            tags: String,
                                            value_state: ValueState}},
                            states: {key: {state: State,
                                           post_id: UInt32}}}

describe Sophia do
  env = TestEnv.new YAML.parse <<-YAML
  sophia:
    path: /tmp/sophia
  db:
    tags: &ddbs
      compression: zstd
      compaction:
        cache: 1_000_000_000
    posts:
      <<: *ddbs
    states:
      <<: *ddbs
  YAML

  tk = {name: "tag"}
  tv = {type: "type"}
  td = {name: "tag", type: "type"}

  pk = {host: "host", id: 1_u32}
  pv = {url: "url", tags: "tags", value_state: ValueState::B}
  pd = {host: "host", id: 1_u32, url: "url", tags: "tags", value_state: ValueState::B}

  sk = {state: State::Rejected, post_id: 4_u32}
  sd = sk

  it "insert documents" do
    env << td << pd << sd # one by one
    env << [td, pd, sd]   # in transaction
  end

  it "check if has key" do
    env.has_key?(tk).should eq true
    env.has_key?(pk).should eq true
    env.has_key?(sk).should eq true
  end

  it "get value by key" do
    env[tk]?.should eq td
    env[pk]?.should eq pd
  end

  it "iterate from key" do
    c = env.cursor tk, ">="
    c.data.should eq nil

    c.next.should eq td
    c.data.should eq td

    c.next.should eq nil
    c.data.should eq nil

    c.next.should eq nil
    c.data.should eq nil
  end

  it "iterate from key using block" do
    t = [] of {name: String, type: String}
    env.from(tk, ">=") do |d|
      t << d
    end
    t.should eq [td]
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

  it "perform arbitrary operations in transaction" do
    env.delete tk
    env.has_key?(tk).should eq false
    begin
      env.transaction do |tx|
        tx << td
        tx.has_key?(tk).should eq true
        raise "oh no"
      end
    rescue
      env.has_key?(tk).should eq false
    end
  end

  env.checkpoint
end
