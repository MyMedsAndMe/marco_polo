defmodule MarcoPolo.Protocol.RecordSerialization do
  @moduledoc false

  alias MarcoPolo.Document
  alias MarcoPolo.RID

  require Record

  Record.defrecordp :map_key, [:key, :data_type, :data_ptr]
  Record.defrecordp :field, [:name, :ptr, :type]

  @doc """
  Decodes a binary-serialized record into a `MarcoPolo.Record` struct.

  This function decodes the bytes representing a record into a
  `MarcoPolo.Record` struct. The record is assumed to be serialized with the
  `ORecordSerializerBinary` serialization. `data` is the full data about the
  record, it has no exceeding data and it's not incomplete. Note that `data`
  represents the bytes for the record content, without the leading int for the
  length of the byte array you would expect from OrientDB's binary
  protocol. This happens because this function is usually called from the parser
  that parsed the byte array.
  """
  @spec decode(binary, Dict.t) :: MarcoPolo.Record.t
  def decode(data, schema \\ %{}) do
    <<_serialization_version, rest :: binary>> = data

    case decode_embedded(rest, schema) do
      {record, <<>>}       -> record
      :unknown_property_id -> :unknown_property_id
    end
  end

  @doc """
  Serializes a given record using the schemaless serialization protocol.

  The record is serialized using the `ORecordSerializerBinary`
  serialization. This function always returns iodata that can be converted to
  binary using `IO.iodata_to_binary/1`.

  This function is the "dual" of `decode/2`, so this is always true:

      decode(encode(record)) = record

  """
  @spec encode(Document.t) :: iodata
  def encode(%Document{} = record) do
    # 0 is the serialization version (as a byte), not the record version.
    [0, encode_embedded(record, 1)]
  end

  # Decodes a document (ODocument). This ODocument can be a "top-level" document
  # or an "embedded" type, since the leading serialization version byte is not
  # decoded here (but in `decode/1`).
  defp decode_embedded(data, schema) do
    {class_name, rest} = decode_type(data, :string)

    case decode_header(rest, schema) do
      {field_definitions, rest} ->
        {fields, rest} = decode_fields(rest, field_definitions, schema)

        if class_name == "" do
          class_name = nil
        end

        {%Document{class: class_name, fields: fields}, rest}
      :unknown_property_id ->
        :unknown_property_id
    end
  end

  defp decode_header(data, schema, acc \\ []) do
    # If `i` is positive, that means the next field definition is a "named
    # field" and `i` is the length of the field's name. If it's negative, it
    # represents the property id of a property. If it's 0, it signals the end of
    # the header segment.

    case :small_ints.decode_zigzag_varint(data) do
      {0, rest} ->
        # Remember to return `rest` and not `data` since `rest` doesn't contain
        # the 0 byte that signals the end of the header, while `data` does.
        {Enum.reverse(acc), rest}
      {i, rest} when i < 0 ->
        case decode_property_definition(rest, i, schema) do
          {field, rest} ->
            decode_header(rest, schema, [field|acc])
          :unknown_property_id ->
            :unknown_property_id
        end
      {i, _} when i > 0 ->
        {field, rest} = decode_field_definition(data)
        decode_header(rest, schema, [field|acc])
    end
  end

  defp decode_property_definition(data, encoded_id, schema) do
    # That's how you decode property ids.
    id = - encoded_id - 1

    case Dict.fetch(schema.global_properties, id) do
      {:ok, {name, type_as_string}} ->
        {ptr, rest} = decode_data_ptr(data)
        field       = field(name: name, type: string_to_type(type_as_string), ptr: ptr)
        {field, rest}
      :error ->
        :unknown_property_id
    end
  end

  # Decodes the definition of a named field in the header (`data`).
  defp decode_field_definition(data) do
    {name, rest}             = decode_type(data, :string)
    {ptr, rest}              = decode_data_ptr(rest)
    <<type, rest :: binary>> = rest

    {field(name: name, type: int_to_type(type), ptr: ptr), rest}
  end

  defp decode_fields(data, field_definitions, schema) do
    {fields, rest} = Enum.map_reduce(field_definitions, data, &decode_field(&2, &1, schema))
    {Enum.into(fields, %{}), rest}
  end

  defp decode_field(data, field(name: name, ptr: 0), _schema) do
    {{name, nil}, data}
  end

  defp decode_field(data, field(name: name, type: type), schema) do
    {value, rest} = decode_type(data, type, schema)
    {{name, value}, rest}
  end

  # The pointer to the data is just a signed int32.
  defp decode_data_ptr(data) do
    <<data_ptr :: 32-signed, rest :: binary>> = data
    {data_ptr, rest}
  end

  # Decodes an instance of `type` from `data`.
  # Made public for testing.
  @doc false
  def decode_type(data, type, schema \\ HashDict.new)

  def decode_type(<<0>> <> rest, :boolean, _), do: {false, rest}
  def decode_type(<<1>> <> rest, :boolean, _), do: {true, rest}

  def decode_type(data, type, _) when type in [:short, :int, :long] do
    :small_ints.decode_zigzag_varint(data)
  end

  def decode_type(data, :float, _) do
    <<float :: 32-float, rest :: binary>> = data
    {float, rest}
  end

  def decode_type(data, :double, _) do
    <<double :: 64-float, rest :: binary>> = data
    {double, rest}
  end

  def decode_type(data, type, _) when type in [:string, :binary] do
    {len, rest} = :small_ints.decode_zigzag_varint(data)
    <<string :: bytes-size(len), rest :: binary>> = rest
    {string, rest}
  end

  def decode_type(data, :datetime, _) do
    {msecs_from_epoch, rest} = decode_type(data, :long)
    secs_from_epoch = div(msecs_from_epoch, 1000)
    msec = rem(msecs_from_epoch, 1000)
    epoch = :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})

    total_secs = epoch + secs_from_epoch
    {{year, month, day}, {hour, min, sec}} = :calendar.gregorian_seconds_to_datetime(total_secs)
    datetime = %MarcoPolo.DateTime{year: year, month: month, day: day,
                                   hour: hour, min: min, sec: sec, msec: msec}

    {datetime, rest}
  end

  def decode_type(data, :embedded, schema) do
    decode_embedded(data, schema)
  end

  def decode_type(data, :embedded_list, schema) do
    {nitems, rest}           = :small_ints.decode_zigzag_varint(data)
    <<type, rest :: binary>> = rest

    # Only ANY is supported by OrientDB at the moment.
    :any = int_to_type(type)

    Enum.map_reduce List.duplicate(nil, nitems), rest, fn(_, <<type, acc :: binary>>) ->
      decode_type(acc, int_to_type(type), schema)
    end
  end

  def decode_type(data, :embedded_set, schema) do
    {elems, rest} = decode_type(data, :embedded_list, schema)
    {Enum.into(elems, HashSet.new), rest}
  end

  def decode_type(data, :embedded_map, schema) do
    {keys, rest}  = decode_map_header(data)
    {pairs, rest} = decode_map_values(rest, keys, schema)

    {Enum.into(pairs, %{}), rest}
  end

  def decode_type(<<cluster_id :: 32, position :: 32, rest :: binary>>, :link, _) do
    {%MarcoPolo.RID{cluster_id: cluster_id, position: position}, rest}
  end

  def decode_type(data, :link_list, _) do
    {nelems, rest} = :small_ints.decode_zigzag_varint(data)
    {elems, rest} = Enum.map_reduce List.duplicate(nil, nelems), rest, fn(_, acc) ->
      decode_type(acc, :link)
    end

    {{:link_list, elems}, rest}
  end

  def decode_type(data, :link_set, _) do
    {{:link_list, elems}, rest} = decode_type(data, :link_list)
    {{:link_set, Enum.into(elems, HashSet.new)}, rest}
  end

  def decode_type(data, :link_map, _) do
    {nkeys, rest} = :small_ints.decode_zigzag_varint(data)
    {pairs, rest} = Enum.map_reduce List.duplicate(0, nkeys), rest, fn(_, <<type, acc :: binary>>) ->
      # Only string keys are supported
      :string = int_to_type(type)

      {key, acc} = decode_type(acc, :string)
      {rid, acc} = decode_type(acc, :link)
      {{key, rid}, acc}
    end

    {{:link_map, Enum.into(pairs, %{})}, rest}
  end

  def decode_type(data, :decimal, _) do
    <<scale :: 32, value_size :: 32, rest :: binary>>         = data
    <<value :: big-size(value_size)-unit(8), rest :: binary>> = rest

    value = value / round(:math.pow(10, scale))
    {Decimal.new(value), rest}
  end

  # TODO: decoding of the LinkBag type, which has... no docs :D

  defp decode_map_header(data) do
    {nkeys, rest} = :small_ints.decode_zigzag_varint(data)

    Enum.map_reduce List.duplicate(nil, nkeys), rest, fn(_, <<string_type, acc :: binary>>) ->
      # For now, OrientDB only supports STRING keys.
      :string = int_to_type(string_type)

      {key, acc} = decode_type(acc, :string)
      {ptr, acc} = decode_data_ptr(acc)
      <<type, acc :: binary>> = acc

      {field(name: key, type: int_to_type(type), ptr: ptr), acc}
    end
  end

  defp decode_map_values(data, keys, schema) do
    Enum.map_reduce(keys, data, &decode_field(&2, &1, schema))
  end

  defp encode_fields(%{} = fields, offset) do
    offset = offset + header_offset(fields)

    acc = {[], [], offset}
    {fields, values, _} = Enum.reduce fields, acc, fn({field_name, field_value}, {fs, vs, index}) ->
      encoded_value = encode_value(field_value, index)
      encoded_field = encode_field_for_header(field_name, index, field_value)
      index         = index + IO.iodata_length(encoded_value)

      {[encoded_field|fs], [encoded_value|vs], index}
    end

    [Enum.reverse(fields), 0, Enum.reverse(values)]
  end

  # Returns the length of the header based on the list of fields.
  defp header_offset(fields) do
    # The last +1 is for the `0` that signals the end of the header.
    fields
    |> Stream.map(fn({name, value}) -> encode_field_for_header(name, 0, value) end)
    |> Stream.map(&IO.iodata_length/1)
    |> Enum.sum
    |> +(1)
  end

  defp encode_embedded(%Document{class: class, fields: fields}, offset) do
    if is_nil(class) do
      class = ""
    end

    encoded_class  = encode_value(class, offset)
    encoded_fields = encode_fields(fields, offset + IO.iodata_length(encoded_class))

    [encoded_class, encoded_fields]
  end

  defp encode_field_for_header(name, ptr, value) do
    type = infer_type(value)

    if is_atom(name) do
      name = Atom.to_string(name)
    end

    if is_nil(value) do
      ptr = 0
      type = :boolean
    end

    [encode_value(name), <<ptr :: 32-signed>>, type_to_int(type)]
  end

  @doc false
  def encode_value(value, offset \\ 0)

  def encode_value({type, value}, offset) do
    encode_type(value, type, offset)
  end

  def encode_value(value, offset) do
    encode_type(value, infer_type(value), offset)
  end

  defp encode_type(value, type, offset)

  defp encode_type(true, :boolean, _offset),  do: <<1>>
  defp encode_type(false, :boolean, _offset), do: <<0>>

  defp encode_type(binary, type, _offset) when type in [:string, :binary] do
    [:small_ints.encode_zigzag_varint(byte_size(binary)), binary]
  end

  defp encode_type(i, type, _offset) when type in [:short, :int, :long] do
    :small_ints.encode_zigzag_varint(i)
  end

  defp encode_type(x, :float, _offset), do: <<x :: 32-float>>
  defp encode_type(x, :double, _offset), do: <<x :: 64-float>>

  defp encode_type(dt, :datetime, _offset) do
    datetime = {{dt.year, dt.month, dt.day}, {dt.hour, dt.min, dt.sec}}
    secs     = :calendar.datetime_to_gregorian_seconds(datetime)
    epoch    = :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})

    encode_type((secs - epoch) * 1000 + dt.msec, :long, 0)
  end

  defp encode_type(record, :embedded, offset) do
    encode_embedded(record, offset)
  end

  defp encode_type(list, :embedded_list, offset) do
    elems = Enum.map list, fn(el) ->
      [type_to_int(infer_type(el)), encode_value(el, offset)]
    end

    [:small_ints.encode_zigzag_varint(length(list)), type_to_int(:any), elems]
  end

  defp encode_type(set, :embedded_set, offset) do
    encode_type(Set.to_list(set), :embedded_list, offset)
  end

  defp encode_type(map, :embedded_map, offset) when is_map(map) do
    offset = offset + map_header_offset(map)

    {keys, values, _} = Enum.reduce map, {[], [], offset}, fn({key, value}, {ks, vs, index}) ->
      encoded_value = <<>>

      if is_nil(value) do
        key = [type_to_int(:string),
               encode_value(to_string(key)),
               <<0 :: 32-signed>>,
               0]
      else
        key = [type_to_int(:string),
               encode_value(to_string(key)),
               <<index :: 32-signed>>,
               type_to_int(infer_type(value))]
        encoded_value = encode_value(value, index)
        index = index + IO.iodata_length(encoded_value)
      end

      {[key|ks], [encoded_value|vs], index}
    end

    keys   = Enum.reverse(keys)
    values = Enum.reverse(values)

    nkeys = map |> map_size |> :small_ints.encode_zigzag_varint

    [nkeys, keys, values]
  end

  defp encode_type(%MarcoPolo.RID{cluster_id: id, position: pos}, :link, _offset) do
    <<id :: 32, pos :: 32>>
  end

  defp encode_type(rids, :link_list, offset) do
    [
      :small_ints.encode_zigzag_varint(length(rids)),
      Enum.map(rids, &encode_type(&1, :link, offset))
    ]
  end

  defp encode_type(rids, :link_set, offset) do
    encode_type(Set.to_list(rids), :link_list, offset)
  end

  defp encode_type(rid_map, :link_map, offset) do
    keys_and_values = Enum.map rid_map, fn {k, v} ->
      [type_to_int(:string),
       encode_value(to_string(k), offset),
       encode_value(v, offset)]
    end

    [:small_ints.encode_zigzag_varint(map_size(rid_map)), keys_and_values]
  end

  # TODO: encoding of the LinkBag type which isn't documented *at all*! Yay!

  defp map_header_offset(map) do
    keys = Map.keys(map)

    # `6` means 4 bytes for the pointer to the data, 1 byte for the data type,
    # and 1 byte for the key type.
    nkeys       = :small_ints.encode_zigzag_varint(Enum.count(keys))
    key_lengths = Enum.map(keys, &(IO.iodata_length(encode_value(to_string(&1))) + 6))

    byte_size(nkeys) + Enum.sum(key_lengths)
  end

  defp infer_type(value)

  defp infer_type(%HashSet{}),               do: :embedded_set
  defp infer_type(%Document{}),      do: :embedded
  defp infer_type(%MarcoPolo.RID{}),         do: :link
  defp infer_type(%MarcoPolo.DateTime{}),    do: :datetime
  defp infer_type(%Decimal{}),               do: :decimal
  defp infer_type(val) when is_boolean(val), do: :boolean
  defp infer_type(val) when is_binary(val),  do: :string
  defp infer_type(val) when is_integer(val), do: :int
  defp infer_type(val) when is_float(val),   do: :double
  defp infer_type(val) when is_list(val),    do: :embedded_list
  defp infer_type(val) when is_map(val),     do: :embedded_map
  defp infer_type(val) when is_nil(val),     do: :boolean # irrelevant
  defp infer_type({type, _value}), do: type

  # http://orientdb.com/docs/last/Types.html
  @types [
    boolean: 0,
    int: 1,
    short: 2,
    long: 3,
    float: 4,
    double: 5,
    datetime: 6,
    string: 7,
    binary: 8,
    embedded: 9,
    embedded_list: 10,
    embedded_set: 11,
    embedded_map: 12,
    link: 13,
    link_list: 14,
    link_set: 15,
    link_map: 16,
    byte: 17,
    transient: 18,
    date: 19,
    custom: 20,
    decimal: 21,
    link_bag: 22,
    any: 23,
  ]

  for {type_name, type_id} <- @types do
    defp int_to_type(unquote(type_id)), do: unquote(type_name)
    defp type_to_int(unquote(type_name)), do: unquote(type_id)
    defp string_to_type(unquote(type_name |> Atom.to_string |> String.upcase)), do: unquote(type_name)
  end
end
