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
