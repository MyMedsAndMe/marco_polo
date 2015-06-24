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

  defmodule Field do
    @type t :: %__MODULE__{
      name: binary,
      type: atom,
      pointer_to_data: non_neg_integer,
      encoded_value: iodata,
    }

    defstruct ~w(name type pointer_to_data value encoded_value)a
  end

  @doc """
  Parses a binary-serialized record.
  """
  @spec decode(binary) :: {non_neg_integer, String.t, %{}}
  def decode(data) do
    <<_version, rest :: binary>> = data
    rest |> decode_document |> elem(0)
  end

  @doc """
  Serializes a record using the binary serialization protocol.

  `class_name` is a string containing the class name of the record being
  encoded. `fields` is a list of `Field` structs.
  """
  def encode({class_name, fields}) do
    version_and_class = [0, encode_type(class_name || "", :string)]
    offset            = IO.iodata_length(version_and_class)
    encoded_fields    = encode_fields(fields, offset)

    [version_and_class, encoded_fields]
  end

  # Decodes a document (ODocument). This ODocument can be a "top-level" document
  # or an "embedded" type, since the leading serialization version byte is not
  # decoded here (but in `decode/1`).
  defp decode_document(data) do
    {class_name, rest}        = decode_type(data, :string)
    {field_definitions, rest} = decode_header(rest)
    {fields, rest}            = decode_fields(rest, field_definitions)

    if class_name == "" do
      class_name = nil
    end

    {{class_name, fields}, rest}
  end

  # Decodes an header returning a list of field definitions (which is a list of
  # `%Field{}` structs).
  defp decode_header(data, acc \\ []) do
    {i, rest} = :small_ints.decode_zigzag_varint(data)

    # If `i` is positive, that means the next field definition is a "named
    # field" and `i` is the length of the field's name. If it's negative, it
    # represents the property id of a property. If it's 0, it signals the end of
    # the header segment.
    cond do
      i == 0 ->
        # Remember to return `rest` and not `data` since `rest` doesn't contain
        # the 0 byte that signals the end of the header, while `data` does; we
        # want to ditch that byte.
        {Enum.reverse(acc), rest}
      i < 0 ->
         raise "properties aren't supported yet, only fields with a name and an explicit type"
      i > 0 ->
        {field, rest} = decode_field_definition(:named_field, data)
        decode_header(rest, [field|acc])
    end
  end

  # Decodes the definition of a named field in the header (`data`). Returns a
  # `%Field{}` struct with an empty value.
  defp decode_field_definition(:named_field, data) do
    {field_name, rest}            = decode_type(data, :string)
    {data_ptr, rest}              = decode_data_ptr(rest)
    <<data_type, rest :: binary>> = rest

    field = %Field{name: field_name, type: int_to_type(data_type), pointer_to_data: data_ptr}
    {field, rest}
  end

  # Decodes fields from the body of a serialized document (`data`) and a list of
  # `%Field{}` structs (with no `:value` field, they're definitions). Returns a
  # list of `%Field{}`s and the rest of the given data.
  defp decode_fields(data, field_definitions) do
    {fields, rest} = Enum.map_reduce field_definitions, data, fn(%Field{} = field, acc) ->
      if field.pointer_to_data == 0 do
        {%{field | value: nil}, acc}
      else
        {value, rest} = decode_type(acc, field.type)
        {%{field | value: value}, rest}
      end
    end
  end

  # The pointer to the data is just a signed int32.
  defp decode_data_ptr(data) do
    <<data_ptr :: 32-signed, rest :: binary>> = data
    {data_ptr, rest}
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
    fields = Enum.map(fields, &encode_field_value/1)
    offset = offset + header_offset(fields)

    {fields, values, _} = Enum.reduce fields, {[], [], offset}, fn(%Field{} = field, {fs, vs, index}) ->
      encoded_field = %{field | pointer_to_data: index} |> encode_field_for_header
      index         = index + IO.iodata_length(field.encoded_value)

      {[encoded_field|fs], [field.encoded_value|vs], index}
    end

    [Enum.reverse(fields), 0, Enum.reverse(values)]
  end

  # Returns the length of the header based on the list of fields.
  defp header_offset(fields) do
    # The last +1 is for the `0` that signals the end of the header.
    fields
    |> Enum.map(&(&1 |> encode_field_for_header |> IO.iodata_length))
    |> Enum.sum
    |> +(1)
  end

  # Returns the given `%Field{}` with the `:encoded_value` field set to the
  # result of encoding the `:value` field.
  defp encode_field_value(%Field{type: type, value: value} = field) do
    %{field | encoded_value: encode_type(value, type)}
  end

  # Encodes the given `%Field{}` for the header, i.e., just the field
  # representation and not the value (name, pointer to data, type). Returns
  # iodata.
  defp encode_field_for_header(%Field{pointer_to_data: ptr, type: type, name: name} = field) do
    if is_nil(ptr) do
      ptr = 0
    end

    [encode_type(name, :string), <<ptr :: 32-signed>>, type_to_int(type)]
  end

  # Encodes an instance of `type`. Returns an iodata instead of a binary.
  # Made public for testing.
  @doc false
  def encode_type(data, type)

  def encode_type(str, :string) do
    [:small_ints.encode_zigzag_varint(byte_size(str)), str]
  end

  def encode_type(i, type) when type in [:sint16, :sint32, :sint64] do
    :small_ints.encode_zigzag_varint(i)
  end

  defp field_name(field) when is_record(field, :named_field), do: named_field(field, :name)

  # http://orientdb.com/docs/last/Types.html
  @types [
    boolean: 0,
    sint32: 1,
    sint16: 2,
    sint64: 3,
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
  end
end
