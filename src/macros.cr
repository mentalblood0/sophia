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
