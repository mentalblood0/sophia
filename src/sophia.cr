require "log"
require "yaml"

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
      Log.debug { "sp_setint(#{o}, \"#{path}\", #{value})" }
      e = LibSophia.setint o, path, value
      raise Exception.new "sp_setint(#{o}, #{path}, #{value}) returned #{e}" if e == -1
    end

    def self.setstring(o : P, path : String, value : String)
      Log.debug { "sp_setstring(#{o}, \"#{path}\", \"#{value}\", #{value.size})" }
      e = LibSophia.setstring o, path, value, value.bytesize
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

    def self.commit(tx : P)
      e = CommitResult.new LibSophia.commit tx
      raise Exception.new "sp_commit(#{tx}) returned #{e}" unless e == CommitResult::Success
    end

    def self.destroy(o : P)
      e = LibSophia.destroy o
      raise Exception.new "sp_destroy(#{o}) returned #{e}" unless (e) == 0
    end
  end

  class Environment
    property tx : Sophia::P?
    property destroy_on_collect : Bool = true
    @env : P = P.null

    def last_error_msg
      begin
        Api.getstring?(@env, "sophia.error").not_nil!
      rescue ex
        "Exception while getting last error message: #{ex}"
      end
    end

    def set(path : String, value : Value)
      Api.set @env, path, value
    end

    def getstring(path : String)
      Api.getstring? @env, path
    end

    def getint(path : String)
      Api.getint? @env, path
    end

    def transaction(&)
      d = self.dup
      d.destroy_on_collect = false
      Sophia.mex ({tx = Api.begin @env}), nil
      d.tx = tx
      begin
        yield d
      rescue ex
        Api.destroy tx
        raise ex
      end
      Sophia.mex ({Api.commit tx}), tx
    end

    def finalize
      Api.destroy @env if @destroy_on_collect
    end
  end

  macro mset(o, d, x)
    {% for key, type in x %}
      {% if type.id.starts_with? "UInt" %}Sophia::Api.setint o, {{key.id.stringify}}, {{d}}[:{{key.id}}]
      {% elsif type.id == "String" %}Sophia::Api.setstring o, {{key.id.stringify}}, {{d}}[:{{key.id}}]
      {% else %}Sophia::Api.setint o, {{key.id.stringify}}, {{d}}[:{{key.id}}].value{% end %}
    {% end %}
  end

  macro mget(o, *xx)
    \{
      {% skip = false %}
      {% for i in (0..xx.size - 1) %}
        {% if skip %}
          {% skip = false %}
        {% else %}
          {% x = xx[i] %}
          {% if x.id.starts_with? "{" %}
            {% for key, type in x %}
              {% if type.id.starts_with? "UInt" %}{{key.id}}: Sophia::Api.getint?({{o}}, {{key.id.stringify}}).not_nil!.to_u{{type.id[4..]}},
              {% elsif type.id == "String" %}{{key.id}}: Sophia::Api.getstring?({{o}}, {{key.id.stringify}}).not_nil!,
              {% else %}{{key.id}}: {{type}}.new(Sophia::Api.getint?({{o}}, {{key.id.stringify}}).not_nil!.to_{{type.resolve.constant(type.resolve.constants.first).kind.id}}),{% end %}
            {% end %}
          {% else %}
            {% for key, _ in xx[i + 1] %}
              {{key.id}}: {{x}}[:{{key}}],
            {% end %}
            {% skip = true %}
          {% end %}
        {% end %}
      {% end %}
    }
  end

  macro mex(b, *oo)
    begin
      {{b}}
    rescue ex : Sophia::Exception
      {% for o in oo %}
        Sophia::Api.destroy {{o}}.not_nil! if {{o}}
      {% end %}
      raise Sophia::Exception.new "#{ex} (last error message is \"#{last_error_msg}\")"
    end
  end

  macro define_env(env_name, s)
    class {{env_name}} < Sophia::Environment
      {% for db_name, _ in s %}@{{db_name}} : Sophia::P = Sophia::P.null
      {% end %}

      protected def flat(data : YAML::Any, prefix = "", result : Hash(String, String | Int64) = {} of String => String | Int64)
        r = data.raw
        case r
        when Hash
          r.each { |k, v| flat v, prefix.empty? ? k.to_s : "#{prefix}.#{k}", result }
        when Array
          r.each { |v| flat v, prefix, result }
        when String, Int64
          result[prefix] = r
        else
          raise Sophia::Exception.new "Not supported type #{typeof(r)} of value #{data.raw}"
        end
        result
      end

      def initialize(settings : YAML::Any)
        @env = Sophia::Api.env
        Sophia.mex (begin
          {% for db_name, db_scheme in s %}
            # db
            Sophia::Api.set @env, "db", {{db_name.stringify}}
            {% kscheme = "db.#{db_name}.scheme" %}

            # fields
            {% for key, _ in db_scheme[:key].keys %}Sophia::Api.set @env, {{kscheme}}, {{key.stringify}}
            {% end %}
            {% if db_scheme[:value] %}{% for key, _ in db_scheme[:value].keys %}Sophia::Api.set @env, {{kscheme}}, {{key.stringify}}
            {% end %}{% end %}

            {% type_to_s = {"String" => "string",
                            "UInt64" => "u64",
                            "UInt32" => "u32",
                            "UInt16" => "u16",
                            "UInt8"  => "u8"} %}

            {% enum_type_to_s = {"u64" => "u64",
                                 "u32" => "u32",
                                 "u16" => "u16",
                                 "u8"  => "u8"} %}

            # key
            {% i = 0 %}
            {% for path_value in db_scheme[:key].to_a %}
              {% if type_to_s.has_key? path_value[1].stringify %}
            Sophia::Api.set @env, "{{kscheme.id}}.{{path_value[0]}}", "{{type_to_s[path_value[1].stringify].id}},key({{i}})"
              {% else %}
                {% crystal_type_s = path_value[1].resolve.constant(path_value[1].resolve.constants.first).kind.id.stringify %}
                {% db_type_s = enum_type_to_s[crystal_type_s].id %}
                {% if db_type_s == "nil" %}{% raise "Sophia does not support underyling type #{crystal_type_s} of enum #{path_value[1]}" %}{% end %}
            Sophia::Api.set @env, "{{kscheme.id}}.{{path_value[0]}}", "{{db_type_s}},key({{i}})"
              {% end %}
              {% i += 1 %}
            {% end %}

            # value
            {% if db_scheme[:value] %}
              {% for path, value in db_scheme[:value] %}
                {% if type_to_s.has_key? value.stringify %}
              Sophia::Api.set @env, "{{kscheme.id}}.{{path}}", "{{type_to_s[value.stringify].id}}"
                {% else %}
                  {% crystal_type_s = value.resolve.constant(value.resolve.constants.first).kind.id.stringify %}
                  {% db_type_s = enum_type_to_s[crystal_type_s].id %}
                  {% if db_type_s == "nil" %}{% raise "Sophia does not support underyling type #{crystal_type_s} of enum #{value}" %}{% end %}
            Sophia::Api.set @env, "{{kscheme.id}}.{{path}}", "{{db_type_s}}"
                {% end %}
                {% i += 1 %}
              {% end %}
            {% end %}
          {% end %}

          flat(settings).each { |k, v| Sophia::Api.set @env, k, v }
          Sophia::Api.open @env

          {% for db_name in s %}@{{db_name}} = Sophia::Api.getobject?(@env, "db.{{db_name}}").not_nil!
          {% end %}
        end), nil
      end

      def checkpoint(waiting_threshold : Time::Span = 5.milliseconds)
        {% for db_name, _ in s %}
          set "db.{{db_name}}.compaction.checkpoint", 0_i64
        {% end %}
        set "scheduler.run", 0_i64

        while ({% for db_name_i in s.keys.map_with_index { |d, i| {d, i} } %}
          {% db_name = db_name_i[0] %}
          {% i = db_name_i[1] %}
          (getint("db.{{db_name}}.scheduler.checkpoint") == 1){% if i < s.size - 1 %} ||{% end %}
          {% end %}
        )
          sleep waiting_threshold
        end
      end

      {% for db_name, db_scheme in s %}
        {% k = db_scheme[:key] %}
        {% if db_scheme[:value] %}
          {% v = db_scheme[:value] %}
        {% else %}
          {% v = {} of Symbol => Type %}
        {% end %}

        {% for key in v.keys %}
          {% if k.keys.includes?(key) %}
            {% raise "Duplicate key #{key} in key and value scheme parts for #{db_name}" %}
          {% end %}
        {% end %}

      def <<(document : {
        {% for key, type in k %}{{key}}: {{type}},{% end %}
        {% for key, type in v %}{{key}}: {{type}},{% end %}
      })
        Sophia.mex ({o = Sophia::Api.document @{{db_name}}}), nil
        target = if @tx
                   @tx.not_nil!
                 else
                   @{{db_name}}
                 end
        Sophia.mex (begin
          Sophia.mset o, document, {{k}}
          {% if db_scheme[:value] %}Sophia.mset o, document, {{v}}{% end %}
          Sophia::Api.set target, o
        end), o
        self
      end

      def has_key?(key : {{k}})
        Sophia.mex ({o = Sophia::Api.document @{{db_name}}}), nil
        target = if @tx
                   @tx.not_nil!
                 else
                   @{{db_name}}
                 end
        Sophia.mex (begin
          Sophia.mset o, key, {{k}}
          if r = Sophia::Api.get?(target, o)
            Sophia::Api.destroy r
            true
          else
            false
          end
        end), o
      end

        {% if db_scheme[:value] %}
      def []?(key : {{k}})
        Sophia.mex ({o = Sophia::Api.document @{{db_name}}}), nil
        target = if @tx
                   @tx.not_nil!
                 else
                   @{{db_name}}
                 end
        Sophia.mex (begin
          Sophia.mset o, key, {{k}}
          r = Sophia::Api.get? target, o
          return nil unless r
          result = Sophia.mget r, key, {{k}}, {{v}}
          Sophia::Api.destroy r
        end), o
        result
      end
        {% end %}

      class {{db_name.id.stringify.titleize.id}}Cursor
        getter data : {
          {% for key, type in k %}{{key}}: {{type}},{% end %}
          {% for key, type in v %}{{key}}: {{type}},{% end %}
        }?

        protected def initialize(@cursor : Sophia::P, @o : Sophia::P?, @env : Sophia::Environment)
        end

        def last_error_msg
          @env.last_error_msg
        end

        def next
          return nil unless @o
          Sophia.mex (begin
            @o = Sophia::Api.get? @cursor, @o.not_nil!
            @data = unless @o
                      nil
                    else
                      Sophia.mget @o.not_nil!, {{k}}{% if db_scheme[:value] %}, {{v}}{% end %}
                    end
          end), nil
        end

        def finalize
          Sophia::Api.destroy @o.not_nil! if @o
          Sophia::Api.destroy @cursor
        end
      end

      def cursor(key : {{k}}, order : String = ">=")
        Sophia.mex ({o = Sophia::Api.document @{{db_name}}}), nil
        Sophia.mex (begin
          Sophia::Api.setstring o, "order", order
          Sophia.mset o, key, {{k}}
        end), o
        {{db_name.id.stringify.titleize.id}}Cursor.new Sophia::Api.cursor(@env), o, self
      end

      def from(key : {{k}}, order : String = ">=", &)
        c = cursor key, order
        while data = c.next
          yield data
        end
        Sophia::Api.destroy data.not_nil! if data
      end

      def delete(key : {{k}})
        Sophia.mex ({o = Sophia::Api.document @{{db_name}}}), nil
          target = if @tx
                     @tx.not_nil!
                   else
                     @{{db_name}}
                   end
        Sophia.mex (begin
          Sophia.mset o, key, {{k}}
          Sophia::Api.delete target, o
        end), o
      end
      {% end %}
      def <<(payload : Array({% for db_scheme_i in s.values.map_with_index { |d, i| {d, i} } %}
        {% k = db_scheme_i[0][:key] %}
        {% if db_scheme_i[0][:value] %}
          {% v = db_scheme_i[0][:value] %}
        {% else %}
          {% v = {} of Symbol => Type %}
        {% end %}
        { {% for key, type in k %}{{key}}: {{type}},{% end %}
        {% for key, type in v %}{{key}}: {{type}},{% end %} }{% if db_scheme_i[1] < s.size - 1 %} |{% end %}
      {% end %}))
        self.transaction do |tx|
          payload.each { |document| tx << document }
        end
      end
    end
  end
end
