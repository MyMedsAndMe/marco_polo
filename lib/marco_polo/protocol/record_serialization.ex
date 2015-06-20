defmodule MarcoPolo.Protocol.RecordSerialization do
  import Record

  # We're creating records instead of structs to avoid creating lots of
  # modules. `:property` identifies a field that is in the schema's metadata
  # (`:id` is the property id) while `:named_field` identifies a "dynamic"
  # field, with a `:name` and a type (`:data_type`). Both records contain a
  # `:data_ptr` field that contains the index of the data structure for that
  # field in the serialized record.
  defrecordp :property, [:id, :data_ptr]
  defrecordp :named_field, [:name, :data_type, :data_ptr]

  defrecordp :map_key, [:key, :data_type, :data_ptr]

  @doc """
  Parses a binary-serialized record.
  """
  @spec decode(binary) :: {non_neg_integer, String.t, %{}}
  def decode(data) do
    <<_version, rest :: binary>> = data
    {record, <<>>} = decode_document(rest)
    record
  end

  @doc """
  Serializes a record using the binary serialization protocol.
  """
  def encode({class_name, fields}) do
    version_and_class = [0, encode_type(class_name || "", :string)]
    offset            = IO.iodata_length(version_and_class)
    encoded_fields    = encode_fields(fields, offset)

    [version_and_class, encoded_fields]
  end

  defp decode_document(data) do
    {class_name, rest}    = decode_type(data, :string)
    {header_fields, rest} = decode_header_fields(rest)
    {fields, rest}        = decode_fields(rest, header_fields)

    if class_name == "" do
      class_name = nil
    end

    {{class_name, fields}, rest}
  end

  defp decode_header_fields(data, acc \\ []) do
    {i, rest} = :small_ints.decode_zigzag_varint(data)

    # If `i` is positive, that means the next field definition is a "named
    # field" and `i` is the length of the field's name. If it's negative, it
    # represents the property id of a property. If it's 0, it signals the end of
    # the header segment.

    cond do
      i == 0 ->
        # Remember to return `rest` and not `data` since we want to ditch the 0
        # byte that signals the end of the header.
        {Enum.reverse(acc), rest}
      i < 0 ->
        {data_ptr, rest} = decode_data_ptr(rest)
        property = property(id: decode_property_id(i), data_ptr: data_ptr)
        decode_header_fields(rest, [property|acc])
      i > 0 ->
        {field_name, rest}            = decode_type(data, :string)
        {data_ptr, rest}              = decode_data_ptr(rest)
        <<data_type, rest :: binary>> = rest
        field = named_field(name: field_name, data_type: int_to_type(data_type), data_ptr: data_ptr)

        decode_header_fields(rest, [field|acc])
    end
  end

  defp decode_fields(data, field_definitions) do
    {fields_and_values, rest} = Enum.map_reduce field_definitions, data, fn
      field, acc when is_record(field, :named_field) ->
        named_field(data_type: type, data_ptr: ptr) = field

        if ptr == 0 do
          {{field, nil}, acc}
        else
          {value, rest} = decode_type(acc, type)
          {{field, value}, rest}
        end
    end

    {fields_to_map(fields_and_values), rest}
  end

  defp fields_to_map(fields_and_values) do
    for {field, value} <- fields_and_values, into: %{} do
      {field_name(field), value}
    end
  end

  # The pointer to the data is just a signed int32.
  defp decode_data_ptr(data) do
    <<data_ptr :: 32-signed, rest :: binary>> = data
    {data_ptr, rest}
  end

  defp decode_property_id(i) do
    (i * -1) - 1
  end

  # Decodes an instance of `type` from `data`.
  defp decode_type(data, type)

  defp decode_type(<<0>> <> rest, :boolean), do: {false, rest}
  defp decode_type(<<1>> <> rest, :boolean), do: {true, rest}

  defp decode_type(data, type) when type in [:sint16, :sint32, :sint64] do
    :small_ints.decode_zigzag_varint(data)
  end

  defp decode_type(data, :float) do
    <<float :: 32-float, rest :: binary>> = data
    {float, rest}
  end

  defp decode_type(data, :double) do
    <<double :: 64-float, rest :: binary>> = data
    {double, rest}
  end

  defp decode_type(data, type) when type in [:string, :bytes] do
    {len, rest} = :small_ints.decode_zigzag_varint(data)
    len = len * 8
    <<string :: bits-size(len), rest :: binary>> = rest
    {string, rest}
  end

  defp decode_type(data, :embedded) do
    decode_document(data)
  end

  defp decode_type(data, :embedded_list) do
    {nitems, rest} = :small_ints.decode_zigzag_varint(data)
    <<type, rest :: binary>> = rest

    # Only ANY is supported by OrientDB at the moment.
    :any = int_to_type(type)

    # OPTIMIZE: I have to find a better (clean) way to `map_reduce` n times
    # instead of mapreducing over a list of n times the number 0, which is
    # uselessly expensive to build and plain useless. A range doesn't work
    # because `Enum.to_list(1..0)` is `[1, 0]` which makes sense, but my
    # 1..nitems has to translate to `[]` so that the mapreducing doesn't
    # actually happen.
    Enum.map_reduce List.duplicate(0, nitems), rest, fn(_, acc) ->
      <<type, acc :: binary>> = acc
      decode_type(acc, int_to_type(type))
    end
  end

  defp decode_type(data, :embedded_set) do
    decode_type(data, :embedded_list)
  end

  defp decode_type(data, :embedded_map) do
    {nkeys, rest} = :small_ints.decode_zigzag_varint(data)

    {keys, rest} = Enum.map_reduce List.duplicate(0, nkeys), rest, fn(_, <<type, acc :: binary>>) ->
      {key, acc} = decode_type(acc, int_to_type(type))
      {data_ptr, acc} = decode_data_ptr(acc)
      <<data_type, acc :: binary>> = acc
      {map_key(key: key, data_type: int_to_type(data_type), data_ptr: data_ptr), acc}
    end

    {keys_and_values, rest} = Enum.map_reduce keys, rest, fn(key, acc) ->
      map_key(key: key_name, data_type: type, data_ptr: ptr) = key

      if ptr == 0 do
        {{key_name, nil}, acc}
      else
        {value, acc} = decode_type(acc, type)
        {{key_name, value}, acc}
      end
    end

    {Enum.into(keys_and_values, %{}), rest}
  end

  # I've been bitten a few times with FunctionClauseErrors because I still haven't
  # defined how to decode all types, so let's leave this here until we know how
  # to decode them all. Makes it easier to debug :).
  defp decode_type(data, type) do
    raise "don't know how to decode #{inspect type} from data: #{inspect data}"
  end

  defp encode_fields(fields, offset) do
    fields =
      for {name, value} <- fields do
        field = named_field(name: name, data_type: :string)
        value = encode_type(value, :string)
        {field, value}
      end

    fields_offset = Enum.map(fields, fn({field, _}) ->
      encode_header_field(field) |> IO.iodata_length
    end) |> Enum.sum

    # The last +1 is for the `0` that signals the end of the header.
    offset = offset + fields_offset + 1

    {fields, values, _} = Enum.reduce fields, {[], [], 0}, fn({field, value}, {fs, vs, index}) ->
      ptr = offset + index
      encoded_field = named_field(field, data_ptr: ptr) |> encode_header_field

      index = index + IO.iodata_length(value)

      {[encoded_field|fs], [value|vs], index}
    end

    fields = Enum.reverse(fields)
    values = Enum.reverse(values)

    [fields, 0, values]
  end

  defp encode_header_field(field) when is_record(field, :named_field) do
    named_field(name: name, data_ptr: ptr, data_type: type) = field

    if is_nil(ptr) do
      ptr = 0
    end

    # TODO: the '7' here is the hardcoded code for the 'string' type.
    [encode_type(name, :string), <<ptr :: 32-signed>>, 7]
  end

  # Encodes an instance of `type`. Returns an iodata instead of a binary.
  defp encode_type(data, type)

  defp encode_type(str, :string) do
    [:small_ints.encode_zigzag_varint(byte_size(str)), str]
  end

  defp field_name(field) when is_record(field, :named_field), do: named_field(field, :name)

  # http://orientdb.com/docs/last/Types.html
  defp int_to_type(0),  do: :boolean
  defp int_to_type(1),  do: :sint32
  defp int_to_type(2),  do: :sint16
  defp int_to_type(3),  do: :sint64
  defp int_to_type(4),  do: :float
  defp int_to_type(5),  do: :double
  defp int_to_type(6),  do: :datetime
  defp int_to_type(7),  do: :string
  defp int_to_type(8),  do: :binary
  defp int_to_type(9),  do: :embedded
  defp int_to_type(10), do: :embedded_list
  defp int_to_type(11), do: :embedded_set
  defp int_to_type(12), do: :embedded_map
  defp int_to_type(13), do: :link
  defp int_to_type(14), do: :link_list
  defp int_to_type(15), do: :link_set
  defp int_to_type(16), do: :link_map
  defp int_to_type(17), do: :embedded_list
  defp int_to_type(18), do: :byte
  defp int_to_type(19), do: :transient
  defp int_to_type(20), do: :custom
  defp int_to_type(21), do: :decimal
  defp int_to_type(22), do: :link_bag
  defp int_to_type(23), do: :any
end
