require "log"

require "./LibSophia.cr"

module Sophia
  alias Payload = Hash(String, String | Int64)
  alias P = Pointer(Void)

  class Exception < Exception
  end

  class Api
    def self.env
      raise Exception.new "sp_env returned NULL" if (r = LibSophia.env) == P.null
      r
    end

    def self.set(o : P, path : String, value : String | Int64)
      if value.is_a? String
        e = LibSophia.setstring o, path, value, value.size
        raise Exception.new "sp_setstring returned #{e}" unless e == 0
      else
        e = LibSophia.setint o, path, value
        raise Exception.new "sp_setint returned #{e}" unless e == 0
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

  class Environment
    @env : P

    def initialize(settings : Payload)
      @env = Api.env
      Api.set @env, settings
      Api.open @env
    end

    def []=(name : String, value : String | Int64)
      Api.set @env, name, value
    end

    def getstring(name : String)
      Api.getstring? @env, name
    end

    def getint(name : String)
      Api.getint? @env, name
    end

    def transaction(&)
      tx = Api.begin @env
      transaction = Transaction.new tx
      yield transaction
      Api.commit tx
    end

    def database?(name : String)
      r = Api.getobject? @env, "db.#{name}"
      return nil unless r
      Database.new r
    end

    def from(db : Database, key : String, order : String = ">=", &)
      cursor = Api.cursor @env
      o = db.document({"key" => key, "order" => order}).o
      while o = Api.get? cursor, o
        yield Api.getstring?(o, "key").not_nil!, Api.getstring?(o, "value")
      end
      Api.destroy cursor
    end

    def finalize
      Api.destroy @env
    end
  end

  class Database
    protected def initialize(@db : P)
    end

    def document(payload : Payload)
      Document.new Api.document(@db), payload
    end

    def <<(doc : Document)
      Api.set @db, doc.o
    end

    def []=(key : String, value : String?)
      if value
        self << document({"key" => key, "value" => value})
      else
        self << document({"key" => key})
      end
    end

    def []?(doc : Document)
      r = Api.get? @db, doc.o
      return nil unless r
      Document.new r
    end

    def []?(key : String)
      self[document({"key" => key})]?
    end

    def [](key : String)
      self[key]?.not_nil!["value"]?
    end

    def [](*keys : String)
      r = {} of String => (String | Int64 | Nil)
      keys.each { |k| r[k] = self[k] }
      r
    end

    def delete(doc : Document)
      Api.delete @db, doc.o
    end

    def delete(key : String)
      delete document({"key" => key})
    end
  end

  class Transaction
    protected def initialize(@tr : P)
    end

    def <<(doc : Document)
      Api.set @tr, doc.o
    end

    def []=(db : Database, key : String, value : String?)
      if value
        self << db.document({"key" => key, "value" => value})
      else
        self << db.document({"key" => key})
      end
    end

    def []?(doc : Document)
      r = Api.get? @tr, doc.o
      return nil unless r
      Document.new r
    end

    def []?(db : Database, key : String)
      self[db.document({"key" => key})]?
    end

    def [](db : Database, key : String)
      self[db, key]?.not_nil!["value"]?
    end

    def delete(doc : Document)
      Api.delete @tr, doc.o
    end

    def delete(db : Database, key : String)
      delete db.document({"key" => key})
    end
  end

  struct Document
    getter o : P

    protected def initialize(@o, payload : Payload = Payload.new)
      Log.debug { "Document.new #{@o} #{payload}" }
      Api.set @o, payload
    end

    def []?(name : String)
      Log.debug { "Document{#{@o}}[\"#{name}\"]" }
      size = Pointer(Int32).malloc 1_u64
      Api.getstring? o, name
    end
  end
end
