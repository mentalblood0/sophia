require "./LibSophia.cr"

module Sophia
  alias Payload = Hash(String, Value | Array(String))
  alias P = Pointer(Void)
  alias Key = String | Int64
  alias Value = String | Int64 | Nil

  class Exception < Exception
  end

  class Api
    def self.env
      raise Exception.new "sp_env returned NULL" if (r = LibSophia.env) == P.null
      r
    end

    def self.set(o : P, path : String, value : Value | Array(String))
      if value.is_a? String
        e = LibSophia.setstring o, path, value, value.size
        raise Exception.new "sp_setstring returned #{e} for {#{path}, #{value}}" unless e == 0
      elsif value.is_a? Int64
        e = LibSophia.setint o, path, value
        raise Exception.new "sp_setint returned #{e}" unless e == 0
      elsif value.is_a? Array(String)
        value.each { |value| set o, path, value }
      end
    end

    def self.set(o : P, payload : Payload)
      payload.each { |name, value| set o, name, value }
    end

    def self.getstring?(o : P, path : String)
      size = Pointer(Int32).malloc 1_u64
      p = LibSophia.getstring o, path, size
      return nil if (p == P.null) || (size.value == 0)
      slice = Slice.new(Pointer(UInt8).new(p.address), size.value)
      slice = slice[0, slice.size - 1] if slice.last == 0
      String.new slice
    end

    def self.getint?(o : P, path : String)
      result = LibSophia.getint o, path
      return nil if result == -1
      result
    end

    def self.getobject?(o : P, path : String)
      r = LibSophia.getobject o, path
      return nil if r == P.null
      r
    end

    def self.open(o : P)
      e = LibSophia.open o
      raise Exception.new "sp_open returned #{e}" unless e == 0
    end

    def self.document(db : P)
      r = LibSophia.document db
      raise Exception.new "sp_document returned NULL" if r == P.null
      r
    end

    enum CommitResult
      Error    = -1
      Success  =  0
      Rollback =  1
      Lock     =  2
    end

    def self.set(o : P, doc : P)
      e = CommitResult.new LibSophia.set o, doc
      raise Exception.new "sp_set returned #{e}" unless e == CommitResult::Success
    end

    def self.get?(o : P, doc : P)
      r = LibSophia.get o, doc
      return nil if r == P.null
      r
    end

    def self.delete(o : P, doc : P)
      e = CommitResult.new LibSophia.delete o, doc
      raise Exception.new "sp_delete returned #{e}" unless e == CommitResult::Success
    end

    def self.cursor(env : P)
      r = LibSophia.cursor env
      raise Exception.new "sp_cursor returned NULL" if r == P.null
      r
    end

    def self.begin(env : P)
      tx = LibSophia.begin env
      raise Exception.new "sp_begin returned NULL" if tx == P.null
      tx
    end

    protected def self.commit(tx : P)
      e = CommitResult.new LibSophia.commit tx
      raise Exception.new "sp_commit returned #{e}" unless e == CommitResult::Success
    end

    protected def self.destroy(o : P)
      e = LibSophia.destroy o
      raise Exception.new "sp_destroy returned #{e}" unless (e) == 0
    end
  end

  alias Multipart = Hash(String, Class)
  alias Scheme = {key: Multipart, value: Multipart}

  class Environment
    getter env : P
    getter schemes = {} of String => Scheme

    def initialize(settings : Payload)
      @env = Api.env
      Api.set @env, settings
      Api.open @env
      settings.each do |skey, svalue|
        next unless svalue.is_a? String
        next unless skey_match = skey.match /^db\.(\w+)\.scheme\.(\w+)$/

        db = skey_match[1]
        key = skey_match[2]
        @schemes[db] = {key: Multipart.new, value: Multipart.new} unless @schemes[db]?

        next unless svalue_match = svalue.match /^(\w+)(,key\(\d+\))?$/
        type = {"string"  => String,
                "u64"     => UInt64,
                "u64_rev" => UInt64,
                "u32"     => UInt32,
                "u32_rev" => UInt32,
                "u16"     => UInt16,
                "u16_rev" => UInt16,
                "u8"      => UInt8,
                "u8_rev"  => UInt8}[svalue_match[1]]
        if svalue_match[2]?
          @schemes[db]["key"][key] = type
        else
          @schemes[db]["value"][key] = type
        end
      end
    end

    def []=(path : String, value : Value)
      Api.set @env, path, value
    end

    def getstring(path : String)
      Api.getstring? @env, path
    end

    def getint(path : String)
      Api.getint? @env, path
    end

    def transaction(&)
      tx = Api.begin @env
      transaction = Transaction.new tx
      yield transaction
      Api.commit tx
    end

    protected def database?(name : String)
      Api.getobject? @env, "db.#{name}"
    end

    def finalize
      Api.destroy @env
    end
  end

  class Database(K, V)
    getter scheme : Scheme
    getter db : P
    @env : P

    def initialize(environment : Environment, name : String)
      @scheme = environment.schemes[name]
      @env = environment.env
      @db = environment.database?(name).not_nil!
    end

    def to_h(payload : K | V)
      payload.to_h.map { |k, v| {k.to_s, v} }.to_h
    end

    def set(o : P, payload : K | V)
      Api.set o, to_h payload
    end

    def []=(key : K, value : V)
      o = Api.document @db
      set o, key
      set o, value
      Api.set @db, o
    end

    protected def to_h(o : P, scheme_symbol : Symbol)
      result = {} of Key => Value
      @scheme[scheme_symbol].each_key do |sym|
        s = sym.to_s
        result[s] = Api.getstring? o, s
      end
      result
    end

    def []?(key : K) : V?
      o = Api.document @db
      set o, key

      r = Api.get? @db, o
      return nil unless r

      V.from to_h r, :value
    end

    def from(key : K, order : String = ">=", &)
      cursor = Api.cursor @env
      o = Api.document @db
      set o, key
      while o = Api.get? cursor, o
        yield ({K.from(to_h o, :key), V.from(to_h o, :value)})
      end
      Api.destroy cursor
    end

    def delete(key : K)
      o = Api.document @db
      set o, key
      Api.delete @db, o
    end
  end

  class Transaction
    protected def initialize(@tr : P)
    end

    def []=(db : Database, key : K, value : V)
      o = Api.document db.db
      set o, key
      set o, value
      Api.set @db, o
    end

    def []?(doc : Document)
      r = Api.get? @tr, doc.o
      return nil unless r
      Document.new r
    end

    def []?(db : Database, key : Key)
      self[db.document({"key" => key})]?
    end

    def [](db : Database, key : Key)
      self[db, key]?.not_nil!["value"]?
    end

    def [](db : Database, *keys : Key)
      r = {} of Key => Value
      keys.each { |k| r[k] = self[db, k] }
      r
    end

    def delete(doc : Document)
      Api.delete @tr, doc.o
    end

    def delete(db : Database, key : Key)
      delete db.document({"key" => key})
    end
  end
end
