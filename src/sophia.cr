require "log"

require "./macros"
require "./LibSophia.cr"

module Sophia
  alias H = Hash(String, Value | Array(String))
  alias P = Pointer(Void)
  alias Key = String | Int64
  alias Value = String | Int64 | UInt64 | UInt32 | UInt16 | UInt8 | Nil

  class Exception < Exception
  end

  class Api
    def self.env
      raise Exception.new "sp_env() returned NULL" if (r = LibSophia.env) == P.null
      r
    end

    def self.set(o : P, path : String, value : Value | Array(String))
      Log.debug { "set #{o}, #{path}, #{value}" }
      if value.is_a? String
        e = LibSophia.setstring o, path, value, value.size
        raise Exception.new "sp_setstring(#{o}, #{path}, #{value}, #{value.size}) returned #{e}" unless e == 0
      elsif value.is_a? Array(String)
        value.each { |value| set o, path, value }
      elsif value.is_a? Nil
      else
        e = LibSophia.setint o, path, value
        raise Exception.new "sp_setint(#{o}, #{path}, #{value}) returned #{e}" unless e == 0
      end
    end

    def self.set(o : P, payload : H)
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

  alias Multipart = Hash(String, String.class | UInt64.class | UInt32.class | UInt16.class | UInt8.class)
  alias Scheme = {key: Multipart, value: Multipart}

  class Environment
    @@type_to_s = {String => "string",
                   UInt64 => "u64",
                   UInt32 => "u32",
                   UInt16 => "u16",
                   UInt8  => "u8"}

    getter env : P

    def initialize(settings : H, *dbs)
      @env = Api.env

      dbs.each do |db|
        Api.set @env, "db", db.name
        kscheme = "db.#{db.name}.scheme"
        (db.scheme[:key].keys + db.scheme[:value].keys).each { |value| Api.set @env, kscheme, value }

        db.scheme[:key].each_with_index { |nv, i| Api.set @env, "#{kscheme}.#{nv[0]}", "#{@@type_to_s[nv[1]]},key(#{i})" }
        db.scheme[:value].each { |n, v| Api.set @env, "#{kscheme}.#{n}", @@type_to_s[v] }

        db.settings.each { |n, v| Api.set @env, "db.#{db.name}.#{n}", v }
      end

      Api.set @env, settings
      Api.open @env

      dbs.each { |db| db.set_environment self }
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

  class Database(K, V)
    getter scheme : Scheme = {key: Multipart.new, value: Multipart.new}
    getter name : String
    getter settings : H

    @transaction : Transaction?
    @environment : Environment?
    @db : P?

    def initialize(@name, @settings = H.new)
      ntt2ht(K).each { |name, type| @scheme[:key][name] = type }
      ntt2ht(V).each { |name, type| @scheme[:value][name] = type }
    end

    protected def set_environment(environment : Environment)
      @environment = environment
      @db = Api.getobject?(@environment.not_nil!.env, "db.#{name}").not_nil!
    end

    protected def set_transaction(transaction : Transaction)
      @transaction = transaction
    end

    def in(transaction : Transaction)
      r = self.dup
      r.set_transaction transaction
      r
    end

    protected def set(o : P, payload : K | V)
      Api.set o, payload.to_h.map { |k, v| {k.to_s, v} }.to_h
    end

    def []=(key : K, value : V)
      o = Api.document @db.not_nil!
      set o, key
      set o, value
      if @transaction
        Api.set @transaction.not_nil!.tr, o
      else
        Api.set @db.not_nil!, o
      end
    end

    protected def to_h(o : P, scheme_symbol : Symbol)
      result = {} of Key => Value
      @scheme[scheme_symbol].each do |sym, type|
        s = sym.to_s
        result[s] = if type == String
                      Api.getstring? o, s
                    else
                      if v = Api.getint? o, s
                        if type == UInt8
                          v.to_u8
                        elsif type == UInt16
                          v.to_u16
                        elsif type == UInt32
                          v.to_u32
                        elsif type == UInt64
                          v.to_u64
                        end
                      end
                    end
      end
      result
    end

    protected def get_o?(key : K)
      o = Api.document @db.not_nil!
      set o, key

      if @transaction
        Api.get? @transaction.not_nil!.tr, o
      else
        Api.get? @db.not_nil!, o
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
      cursor = Api.cursor @environment.not_nil!.env
      o = Api.document @db.not_nil!
      set o, key
      while o = Api.get? cursor, o
        yield ({K.from(to_h o, :key), V.from(to_h o, :value)})
      end
      Api.destroy cursor
    end

    def delete(key : K)
      o = Api.document @db.not_nil!
      set o, key
      if @transaction
        Api.delete @transaction.not_nil!.tr, o
      else
        Api.delete @db.not_nil!, o
      end
    end
  end
end
