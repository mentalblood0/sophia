require "log"

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

    def self.setint(o : P, path : String, value : UInt64 | UInt32 | UInt16 | UInt8 | Int64)
      Log.debug { "so_setint(#{o}, \"#{path}\", #{value})" }
      e = LibSophia.setint o, path, value
      raise Exception.new "sp_setint(#{o}, #{path}, #{value}) returned #{e}" unless e == 0
    end

    def self.setstring(o : P, path : String, value : String)
      Log.debug { "so_setstring(#{o}, \"#{path}\", \"#{value}\", #{value.size})" }
      e = LibSophia.setstring o, path, value, value.size
      raise Exception.new "sp_setstring(#{o}, #{path}, #{value}, #{value.size}) returned #{e}" unless e == 0
    end

    def self.set(o : P, path : String, value : Value | Array(String))
      if value.is_a? String
        setstring o, path, value
      elsif value.is_a? Array(String)
        value.each { |value| set o, path, value }
      elsif value.is_a? Nil
      else
        setint o, path, value
      end
    end

    def self.set(o : P, payload : H)
      payload.each { |name, value| set o, name, value }
    end

    def self.getstring?(o : P, path : String)
      size = Pointer(Int32).malloc 1_u64
      p = LibSophia.getstring o, path, size
      return nil if p == P.null
      return "" if size.value == 0
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

  class Environment
    getter env : P = P.null

    def last_error_msg
      Api.getstring?(@env, "sophia.error").not_nil!
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
      yield tx
      Api.commit tx
    end

    def finalize
      Api.destroy @env
    end
  end

  macro define_env(env_name, s)
    class {{env_name}} < Sophia::Environment


      @dbs : \{
              {% for db_name, db_scheme in s %}  {{db_name}}: Sophia::Database({{db_scheme[:key]}}, {{db_scheme[:value]}}),
              {% end %}}?

      def initialize(settings : Sophia::H, dbs_settings : NamedTuple({% for db_name, _ in s %} {{db_name}}: Sophia::H, {% end %}))
        @env = Sophia::Api.env
        begin
          {% for db_name, db_scheme in s %}
            # db
            Sophia::Api.set @env, "db", {{db_name.stringify}}
            {% kscheme = "db.#{db_name}.scheme" %}
            # fields
            {% for key, _ in db_scheme[:key].keys %}Sophia::Api.set @env, {{kscheme}}, {{key.stringify}}
            {% end %}
            {% for key, _ in db_scheme[:value].keys %}Sophia::Api.set @env, {{kscheme}}, {{key.stringify}}
            {% end %}

            {% type_to_s = {"String" => "string",
                            "UInt64" => "u64",
                            "UInt32" => "u32",
                            "UInt16" => "u16",
                            "UInt8"  => "u8"} %}
            # key
            {% for path_value in db_scheme[:key].to_a.sort_by { |k, _| k.stringify }.map_with_index { |nv, i| {"#{kscheme.id}.#{nv[0]}", "#{type_to_s[nv[1].stringify].id},key(#{i})"} } %}Sophia::Api.set @env, {{path_value[0]}}, {{path_value[1]}}
            {% end %}
            # value
            {% for n, v in db_scheme[:value] %}Sophia::Api.set @env, "{{kscheme.id}}.{{n}}", {{type_to_s[nv[1].stringify]}}
            {% end %}
            # settings
            dbs_settings[:{{db_name}}].each { |k, v| Sophia::Api.set @env, "db.{{db_name}}.#{k}", v }
          {% end %}

          Sophia::Api.set @env, settings
          Sophia::Api.open @env

          @dbs = \{
          {% for db_name, db_scheme in s %}  {{db_name}}: Sophia::Database({{db_scheme[:key]}}, {{db_scheme[:value]}}).new(self, Sophia::Api.getobject?(@env, "db.{{db_name}}").not_nil!),
          {% end %}}
        rescue ex : Sophia::Exception
          raise Sophia::Exception "#{ex} (last error message is \"#{last_error_msg}\")"
        end
      end

    {% for db_name, _ in s %}
    def {{db_name}}
      @dbs.not_nil![:{{db_name}}]
    end
    {% end %}
    end
  end

  macro ntt2ht(named_tuple_type)
    {% if named_tuple_type.resolve? %}
      {% if named_tuple_type.resolve < NamedTuple %}
        {% hash_entries = named_tuple_type.resolve.keys.map do |key|
             %("#{key}") + " => " + "#{named_tuple_type.resolve[key]}"
           end %}
        Hash{ {{hash_entries.join(", ").id}} }
      {% else %}
        {{ raise "Type must be a NamedTuple" }}
      {% end %}
    {% else %}
      {{ raise "Type not found" }}
    {% end %}
  end

  class Database(K, V)
    property tx : P?

    def initialize(@environment : Environment, @db : P)
    end

    macro mget(o, t)
      \{
        {% for key, type in t.resolve %}
          {% if type.id.starts_with? "UInt" %}{{key.id}}: Api.getint?({{o}}, {{key.id.stringify}}).not_nil!.to_u{{type.id[4..]}},
          {% elsif type.id == "String" %}{{key.id}}: Api.getstring?({{o}}, {{key.id.stringify}}).not_nil!,{% end %}
        {% end %}
      }
    end

    macro mset(o, v, t)
      {% for key, type in t.resolve %}
        {% if type.id.starts_with? "UInt" %}Api.setint o, {{key.id.stringify}}, {{v}}[:{{key.id}}]
        {% elsif type.id == "String" %}Api.setstring o, {{key.id.stringify}}, {{v}}[:{{key.id}}]{% end %}
      {% end %}
    end

    macro iftx(method, o)
      if @tx
        Api.{{method}} @tx.not_nil!, {{o}}
      else
        Api.{{method}} @db.not_nil!, {{o}}
      end
    end

    def []=(key : K, value : V)
      o = Api.document @db
      mset o, key, K
      mset o, value, V
      iftx set, o
    end

    protected def get_o?(key : K)
      o = Api.document @db
      mset o, key, K
      iftx get?, o
    end

    def has_key?(key : K)
      get_o?(key) != nil
    end

    def []?(key : K)
      o = get_o? key
      return nil unless o
      mget o, V
    end

    def from(key : K, order : String = ">=", &)
      cursor = Api.cursor @environment.env
      o = Api.document @db
      mset o, key, K
      while o = Api.get? cursor, o
        yield mget(o, K), mget(o, V)
      end
      Api.destroy cursor
    end

    def delete(key : K)
      o = Api.document @db
      mset o, key, K
      iftx delete, o
    end
  end
end
