require "./macros"

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
      raise Exception.new "sp_env() returned NULL" if (r = LibSophia.env) == P.null
      r
    end

    def self.set(o : P, path : String, value : Value | Array(String))
      if value.is_a? String
        e = LibSophia.setstring o, path, value, value.size
        raise Exception.new "sp_setstring(#{o}, #{path}, #{value}, #{value.size}) returned #{e}" unless e == 0
      elsif value.is_a? Int64
        e = LibSophia.setint o, path, value
        raise Exception.new "sp_setint(#{o}, #{path}, #{value}) returned #{e}" unless e == 0
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
      raise Exception.new "sp_open(#{o}) returned #{e}" unless e == 0
    end

    def self.document(db : P)
      r = LibSophia.document db
      raise Exception.new "sp_document(#{db}) returned NULL" if r == P.null
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
      raise Exception.new "sp_set(#{o}, #{doc}) returned #{e}" unless e == CommitResult::Success
    end

    def self.get?(o : P, doc : P)
      r = LibSophia.get o, doc
      return nil if r == P.null
      r
    end

    def self.delete(o : P, doc : P)
      e = CommitResult.new LibSophia.delete o, doc
      raise Exception.new "sp_delete(#{o}, #{doc}) returned #{e}" unless e == CommitResult::Success
    end

    def self.cursor(env : P)
      r = LibSophia.cursor env
      raise Exception.new "sp_cursor(#{env}) returned NULL" if r == P.null
      r
    end

    def self.begin(env : P)
      tx = LibSophia.begin env
      raise Exception.new "sp_begin(#{env}) returned NULL" if tx == P.null
      tx
    end

    protected def self.commit(tx : P)
      e = CommitResult.new LibSophia.commit tx
      raise Exception.new "sp_commit(#{tx}) returned #{e}" unless e == CommitResult::Success
    end

    protected def self.destroy(o : P)
      e = LibSophia.destroy o
      raise Exception.new "sp_destroy(#{o}) returned #{e}" unless (e) == 0
    end
  end

  alias Multipart = Hash(String, Class)
  alias Scheme = {key: Multipart, value: Multipart}

  def self.scheme_conf(db_name : String, key : Multipart, value : Multipart)
    type_to_s = {String => "string",
                 UInt64 => "u64",
                 UInt32 => "u32",
                 UInt16 => "u16",
                 UInt8  => "u8"}
    r = {} of String => (String | Array(String))

    kscheme = "db.#{db_name}.scheme"
    r[kscheme] = key.keys + value.keys

    key.each_with_index { |nv, i| r["#{kscheme}.#{nv[0]}"] = "#{type_to_s[nv[1]]},key(#{i})" }
    value.each { |n, v| r["#{kscheme}.#{n}"] = type_to_s[v] }
    r
  end

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
      yield Transaction.new tx
      Api.commit tx
    end

    def finalize
      Api.destroy @env
    end
  end

  struct Transaction
    getter tr : P

    def initialize(@tr)
    end
  end

  struct Database(K, V)
    @scheme : Scheme
    @db : P
    @transaction : Transaction?

    def initialize(@environment : Environment, @name : String, @transaction = nil)
      @scheme = environment.schemes[name]
      raise Exception.new "Scheme from environment config #{@scheme[:key]} do not match template argument #{K} for database #{name}" if @scheme[:key] != ntt2ht(K)
      raise Exception.new "Scheme from environment config #{@scheme[:value]} do not match template argument #{V} for database #{name}" if @scheme[:value] != ntt2ht(V)
      @db = Api.getobject?(@environment.env, "db.#{name}").not_nil!
    end

    def in(transaction : Transaction)
      Database(K, V).new @environment, @name, transaction
    end

    protected def set(o : P, payload : K | V)
      Api.set o, payload.to_h.map { |k, v| {k.to_s, v} }.to_h
    end

    def []=(key : K, value : V)
      o = Api.document @db
      set o, key
      set o, value
      if @transaction
        Api.set @transaction.not_nil!.tr, o
      else
        Api.set @db, o
      end
    end

    protected def to_h(o : P, scheme_symbol : Symbol)
      result = {} of Key => Value
      @scheme[scheme_symbol].each_key do |sym|
        s = sym.to_s
        result[s] = Api.getstring? o, s
      end
      result
    end

    protected def get_o?(key : K)
      o = Api.document @db
      set o, key

      if @transaction
        Api.get? @transaction.not_nil!.tr, o
      else
        Api.get? @db, o
      end
    end

    def has_key?(key : K)
      get_o?(key) != nil
    end

    def []?(key : K) : V?
      o = get_o? key
      return nil unless o
      V.from to_h o, :value
    end

    def from(key : K, order : String = ">=", &)
      cursor = Api.cursor @environment.env
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
      if @transaction
        Api.delete @transaction.not_nil!.tr, o
      else
        Api.delete @db, o
      end
    end
  end
end
