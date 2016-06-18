defmodule MarcoPolo.Protocol.RecordSerialization do
  @moduledoc false

  # This module implements the serialization/deserialization protocol described
  # here:
  # http://orientdb.com/docs/last/Record-Schemaless-Binary-Serialization.html. It's
  # a little bit of a mess.

  require Record
  Record.defrecordp :field, [:name, :ptr, :type]
  @typep field :: {:field, binary, integer, atom}

  @typep schema :: HashDict.t | %{}

  defmodule State do
    @type t :: %__MODULE__{whole_data: binary, schema: HashDict.t | %{}}
    defstruct schema: %{}, whole_data: nil
  end

  alias MarcoPolo.Document
  alias MarcoPolo.RID
  alias MarcoPolo.Protocol.Utils

  import MarcoPolo.Protocol.Protobuf

  @simple_types ~w(boolean
                   short
                   int
                   long
                   float
                   double
                   string
                   binary
                   date
                   datetime
                   decimal
                   link
                   link_list
                   link_set
                   link_map
                   link_bag)a

  @embedded_types ~w(embedded
                     embedded_list
                     embedded_set
                     embedded_map)a

  @doc """
  Decodes a binary-serialized record into a `MarcoPolo.Record` struct.

  This function decodes the bytes representing a record into a
  `MarcoPolo.Record` struct. The record is assumed to be serialized with the
  `ORecordSerializerBinary` serialization. `data` is the full data about the
  record, it has no exceeding data and it's not incomplete. Note that `data`
  represents the bytes for the record content, without the leading int for the
  length of the byte array you would expect from OrientDB's binary
  protocol. This happens because this function is usually called from the parser
  that parsed the byte array. This also means we don't have to care about
  possibly incomplete parts of the serialized records because that would have
  been detected by the parser (that knows the size of the serialized binary).
  """
  @spec decode(binary, Dict.t) :: Document.t | :unknown_property_id
  def decode(data, schema \\ %{}) do
    state = %State{whole_data: data, schema: schema}

    <<_serialization_version, rest :: binary>> = data

    # OrientDB sometimes sends stuff after a record that they use to keep track
    # of updates and other things. Let's ignore this stuff and hope everything
    # goes fine, shall we?
    case decode_embedded(rest, state) do
      {record, _cruft} ->
        record
      :unknown_property_id ->
        :unknown_property_id
    end
  end

  @doc """
  Serializes a given record using the schemaless serialization protocol.

  The record is serialized using the `ORecordSerializerBinary`
  serialization. This function always returns iodata that can be converted to
  binary using `IO.iodata_to_binary/1`.

  This function is the "dual" of `decode/2`, so this is generally true:

      decode(encode(record)) = record

  """
  @spec encode(MarcoPolo.record) :: iodata
  def encode(%Document{} = record) do
    # 0 is the serialization version (as a byte), not the record version.
    [0, encode_embedded(record, 1)]
  end

  # Decodes a document (ODocument). This ODocument can be a "top-level" document
  # or an "embedded" type, since the leading serialization version byte is not
  # decoded here (but in `decode/2`).
  defp decode_embedded(data, %State{} = state) do
    {class_name, rest} = decode_type(data, :string)
    class_name = nullify_empty_string(class_name)

    case decode_header(rest, state) do
      {[], rest} ->
        {%Document{class: class_name, fields: %{}}, rest}
      {field_definitions, _rest} ->
        {fields, rest} = decode_fields(field_definitions, state)
        {%Document{class: class_name, fields: fields}, rest}
      :unknown_property_id ->
        :unknown_property_id
    end
  end

  # Decodes the header of this record (which contains field definitions and
  # "pointers" to the corresponding data in the rest of the binary. Returns a
  # tuple with a list of fields as the first element and the non-header data as
  # the second element.
  @spec decode_header(binary, State.t, [field]) :: [field]
  defp decode_header(data, %State{} = state, acc \\ []) do
    # If `i` is positive, that means the next field definition is a "named
    # field" and `i` is the length of the field's name. If it's negative, it
    # represents the property id of a property. If it's 0, it signals the end of
    # the header segment.
    case decode_zigzag_varint(data) do
      {0, rest} ->
        {Enum.reverse(acc), rest}
      {i, rest} when i < 0 ->
        case decode_property_definition(rest, i, state.schema) do
          {field, rest}        -> decode_header(rest, state, [field|acc])
          :unknown_property_id -> :unknown_property_id
        end
      {i, _} when i > 0 ->
        {field, rest} = decode_field_definition(data)
        decode_header(rest, state, [field|acc])
    end
  end

  @spec decode_property_definition(binary, integer, schema) :: {field, binary} | :unknown_property_id
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

  @spec decode_field_definition(binary) :: {field, binary}
  defp decode_field_definition(data) do
    {name, rest}             = decode_type(data, :string)
    {ptr, rest}              = decode_data_ptr(rest)
    <<type, rest :: binary>> = rest

    {field(name: name, type: int_to_type(type), ptr: ptr), rest}
  end

  @spec decode_fields([field], State.t) :: {%{}, binary}
  defp decode_fields(field_definitions, %State{} = state) when is_list(field_definitions) do
    fields = Enum.map(field_definitions, &decode_field(&1, state))
    {fields_to_map(fields), get_rest_from_list_of_fields(fields)}
  end

  @spec decode_field(field, State.t) :: {{binary, term}, binary}
  defp decode_field(field, state)

  # A 0 pointer means the field is null.
  defp decode_field(field(name: name, ptr: 0), %State{}) do
    {{name, nil}, <<>>}
  end

  defp decode_field(field(name: name, type: type, ptr: ptr), %State{} = state) do
    {value, rest} = decode_type(pointed_data(state.whole_data, ptr), type, state)
    {{name, value}, rest}
  end

  # The pointer to the data is just a signed int32.
  defp decode_data_ptr(data) do
    <<data_ptr :: 32-signed, rest :: binary>> = data
    {data_ptr, rest}
  end

  # Made public for testing.
  @doc false
  @spec decode_type(binary, atom) :: {term, binary}
  def decode_type(pointed_data, type) when type in @simple_types do
    decode_simple_type(pointed_data, type)
  end

  # Made public for testing.
  @doc false
  @spec decode_type(binary, atom, State.t) :: {term, binary}
  def decode_type(pointed_data, type, _state) when type in @simple_types,
    do: decode_simple_type(pointed_data, type)
  def decode_type(pointed_data, type, state) when type in @embedded_types,
    do: decode_embedded_type(pointed_data, type, state)

  @spec decode_simple_type(binary, atom) :: {term, binary}
  defp decode_simple_type(pointed_data, type)

  defp decode_simple_type(<<0>> <> rest, :boolean), do: {false, rest}
  defp decode_simple_type(<<1>> <> rest, :boolean), do: {true, rest}

  defp decode_simple_type(pointed_data, type) when type in [:short, :int, :long] do
    decode_zigzag_varint(pointed_data)
  end

  defp decode_simple_type(pointed_data, :float) do
    <<float :: 32-float, rest :: binary>> = pointed_data
    {float, rest}
  end

  defp decode_simple_type(pointed_data, :double) do
    <<double :: 64-float, rest :: binary>> = pointed_data
    {double, rest}
  end

  # Strings and binaries are encoded/decoded in the exact same way.
  defp decode_simple_type(pointed_data, type) when type in [:string, :binary] do
    {len, rest} = decode_zigzag_varint(pointed_data)
    <<str :: bytes-size(len), rest :: binary>> = rest
    {str, rest}
  end

  defp decode_simple_type(pointed_data, :date) do
    {days, rest} = decode_zigzag_varint(pointed_data)
    days = :calendar.date_to_gregorian_days(1970, 1, 1) + days
    {y, m, d} = :calendar.gregorian_days_to_date(days)
    {%MarcoPolo.Date{year: y, month: m, day: d}, rest}
  end

  defp decode_simple_type(pointed_data, :datetime) do
    {msecs_from_epoch, rest} = decode_simple_type(pointed_data, :long)
    secs_from_epoch = div(msecs_from_epoch, 1000)
    msec = rem(msecs_from_epoch, 1000)
    epoch = :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})

    total_secs = epoch + secs_from_epoch
    {{year, month, day}, {hour, min, sec}} = :calendar.gregorian_seconds_to_datetime(total_secs)

    {%MarcoPolo.DateTime{year: year, month: month, day: day,
                         hour: hour, min: min, sec: sec, msec: msec},
     rest}
  end

  defp decode_simple_type(data, :decimal) do
    <<scale :: 32, value_size :: 32, rest :: binary>>         = data
    <<value :: big-size(value_size)-unit(8), rest :: binary>> = rest

    value = value / round(:math.pow(10, scale))
    {Decimal.new(value), rest}
  end

  defp decode_simple_type(data, :link) do
    {cluster_id, rest} = decode_zigzag_varint(data)
    {position, rest} = decode_zigzag_varint(rest)
    {%RID{cluster_id: cluster_id, position: position}, rest}
  end

  defp decode_simple_type(data, :link_list) do
    {nelems, rest} = decode_zigzag_varint(data)
    {elems, rest} = Utils.reduce_n_times(nelems, rest, &decode_simple_type(&1, :link))
    {{:link_list, elems}, rest}
  end

  defp decode_simple_type(data, :link_set) do
    {{:link_list, elems}, rest} = decode_simple_type(data, :link_list)
    {{:link_set, Enum.into(elems, HashSet.new)}, rest}
  end

  defp decode_simple_type(data, :link_map) do
    {nkeys, rest} = decode_zigzag_varint(data)
    {pairs, rest} = Utils.reduce_n_times nkeys, rest, fn(<<type>> <> acc) ->
      # Only string keys are supported
      :string = int_to_type(type)

      {key, acc} = decode_simple_type(acc, :string)
      {rid, acc} = decode_simple_type(acc, :link)
      {{key, rid}, acc}
    end

    {{:link_map, Enum.into(pairs, %{})}, rest}
  end

  # 1 means "embedded" RidBag.
  defp decode_simple_type(<<1, size :: 32, rest :: binary>>, :link_bag) do
    {rids, rest} = Utils.reduce_n_times size, rest, fn(acc) ->
      <<cluster_id :: 16, position :: 64>> <> acc = acc
      {%RID{cluster_id: cluster_id, position: position}, acc}
    end

    {{:link_bag, rids}, rest}
  end

  defp decode_simple_type(<<0, _ :: binary>>, :link_bag) do
    raise MarcoPolo.Error, """
    Tree-based RidBags are not supported by MarcoPolo (yet); only embedded
    RidBags are. You can change your OrientDB server configuration to force
    OrientDB to use embedded RidBags over tree-based ones. To learn more about
    changing the server configuration, visit
    http://orientdb.com/docs/last/Configuration.html.  The setting to change is
    `ridBag.embeddedToSbtreeBonsaiThreshold`: set it to a very high value to
    ensure OrientDB uses embedded RidBags up to that number of relations. For
    example:

        <properties>
          ...
          <entry name="ridBag.embeddedToSbtreeBonsaiThreshold" value="1000000000" />
        </properties>

    Note that for this configuration to take effect for a database, that
    database must be created after this configuration is set on the server.
    """
  end

  @spec decode_embedded_type(binary, atom, State.t) :: {term, binary}
  defp decode_embedded_type(pointed_data, type, state)

  defp decode_embedded_type(pointed_data, :embedded, %State{} = state) do
    decode_embedded(pointed_data, state)
  end

  defp decode_embedded_type(pointed_data, :embedded_list, %State{} = state) do
    {nitems, rest}           = decode_zigzag_varint(pointed_data)
    <<type, rest :: binary>> = rest

    # Only ANY is supported by OrientDB at the moment.
    :any = int_to_type(type)

    Utils.reduce_n_times nitems, rest, fn(<<type>> <> acc) ->
      decode_type(acc, int_to_type(type), state)
    end
  end

  defp decode_embedded_type(pointed_data, :embedded_set, %State{} = state) do
    {elems, rest} = decode_embedded_type(pointed_data, :embedded_list, state)
    {Enum.into(elems, HashSet.new), rest}
  end

  defp decode_embedded_type(pointed_data, :embedded_map, %State{} = state) do
    decode_embedded_map(pointed_data, state)
  end

  defp decode_embedded_map(pointed_data, state) do
    case decode_map_keys(pointed_data) do
      {[], rest}    -> {%{}, rest}
      {keys, _rest} -> decode_map_values(keys, state)
    end
  end

  defp decode_map_keys(data) do
    {nkeys, rest} = decode_zigzag_varint(data)

    Utils.reduce_n_times nkeys, rest, fn(<<type>> <> acc) ->
      # For now, OrientDB only supports STRING keys.
      :string = int_to_type(type)

      {key, acc} = decode_simple_type(acc, :string)
      {ptr, acc} = decode_data_ptr(acc)
      <<type, acc :: binary>> = acc

      {field(name: key, type: int_to_type(type), ptr: ptr), acc}
    end
  end

  defp decode_map_values(keys, %State{} = state) do
    values = Enum.map(keys, &decode_field(&1, state))
    rest = get_rest_from_list_of_fields(values)
    values = fields_to_map(values)
    {values, rest}
  end

  defp encode_fields(fields, offset) when is_map(fields) do
    offset = offset + header_offset(fields)

    acc = {[], [], offset}
    {fields, values, _} = Enum.reduce fields, acc, fn({field_name, field_value}, {fs, vs, index}) ->
      encoded_value =
        if is_nil(field_value) do
          <<>>
        else
          encode_value(field_value, index)
        end

      encoded_field = encode_field_for_header(field_name, index, field_value)
      index         = index + IO.iodata_length(encoded_value)

      {[encoded_field|fs], [encoded_value|vs], index}
    end

    [Enum.reverse(fields), 0, Enum.reverse(values)]
  end

  defp header_offset(fields) do
    # The last +1 is for the `0` that signals the end of the header.
    fields
    |> Stream.map(fn({name, value}) -> encode_field_for_header(name, 0, value) end)
    |> Stream.map(&IO.iodata_length/1)
    |> Enum.sum
    |> Kernel.+(1)
  end

  defp encode_embedded(%Document{class: class, fields: fields}, offset) do
    class = if is_nil(class), do: "", else: class
    encoded_class  = encode_value(class)
    encoded_fields = encode_fields(fields, offset + IO.iodata_length(encoded_class))

    [encoded_class, encoded_fields]
  end

  defp encode_field_for_header(name, ptr, value) do
    type = infer_type(value)
    name = to_string(name)

    {ptr, type} =
      if is_nil(value) do
        {0, :boolean}
      else
        {ptr, type}
      end

    [encode_value(name), <<ptr :: 32-signed>>, type_to_int(type)]
  end

  # Encodes a value inferring its type.
  # Made public for testing.
  @doc false
  def encode_value(value, offset \\ 0)

  def encode_value({type, value}, offset),
    do: encode_type(value, type, offset)
  def encode_value(value, offset),
    do: encode_type(value, infer_type(value), offset)

  defp encode_type(value, type, offset)

  defp encode_type(true, :boolean, _offset),  do: <<1>>
  defp encode_type(false, :boolean, _offset), do: <<0>>

  defp encode_type(binary, type, _offset) when type in [:string, :binary] do
    [encode_zigzag_varint(byte_size(binary)), binary]
  end

  defp encode_type(i, type, _offset) when type in [:short, :int, :long] do
    encode_zigzag_varint(i)
  end

  defp encode_type(x, :float, _offset), do: <<x :: 32-float>>
  defp encode_type(x, :double, _offset), do: <<x :: 64-float>>

  defp encode_type(date, :date, _offset) do
     import :calendar, only: [date_to_gregorian_days: 3]
     days = date_to_gregorian_days(date.year, date.month, date.day) - date_to_gregorian_days(1970, 1, 1)
     encode_zigzag_varint(days)
  end

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

    [encode_zigzag_varint(length(list)), type_to_int(:any), elems]
  end

  defp encode_type(set, :embedded_set, offset) do
    encode_type(Set.to_list(set), :embedded_list, offset)
  end

  defp encode_type(map, :embedded_map, offset) when is_map(map) do
    offset = offset + map_header_offset(map)

    {keys, values, _} = Enum.reduce map, {[], [], offset}, fn({key, value}, {ks, vs, index}) ->
      encoded_value = <<>>

      {key, index, encoded_value} =
        if is_nil(value) do
          key = [type_to_int(:string),
                 encode_value(to_string(key)),
                 <<0 :: 32-signed>>,
                 0]
          {key, index, encoded_value}
        else
          key = [type_to_int(:string),
                 encode_value(to_string(key)),
                 <<index :: 32-signed>>,
                 type_to_int(infer_type(value))]
          encoded_value = encode_value(value, index)
          index = index + IO.iodata_length(encoded_value)
          {key, index, encoded_value}
        end

      {[key|ks], [encoded_value|vs], index}
    end

    keys   = Enum.reverse(keys)
    values = Enum.reverse(values)

    nkeys = map |> map_size() |> encode_zigzag_varint()

    [nkeys, keys, values]
  end

  defp encode_type(%RID{cluster_id: id, position: pos}, :link, _offset) do
    encode_zigzag_varint(id) <> encode_zigzag_varint(pos)
  end

  defp encode_type(rids, :link_list, offset) do
    [
      encode_zigzag_varint(length(rids)),
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

    [encode_zigzag_varint(map_size(rid_map)), keys_and_values]
  end

  defp encode_type(rids, :link_bag, _) when is_list(rids) do
    encoded_rids = Enum.map rids, fn(%RID{cluster_id: cluster_id, position: position}) ->
      <<cluster_id :: 16, position :: 64>>
    end

    [1, <<length(rids) :: 32>>|encoded_rids]
  end

  defp map_header_offset(map) do
    keys = Map.keys(map)

    # `6` means 4 bytes for the pointer to the data, 1 byte for the data type,
    # and 1 byte for the key type.
    nkeys       = encode_zigzag_varint(Enum.count(keys))
    key_lengths = Enum.map(keys, &(IO.iodata_length(encode_value(to_string(&1))) + 6))

    byte_size(nkeys) + Enum.sum(key_lengths)
  end

  defp pointed_data(data, position) when position > 0 and byte_size(data) > position,
    do: binary_part(data, position, byte_size(data) - position)
  defp pointed_data(data, pos),
    do: raise(ArgumentError, "position #{pos} is outside of the given binary (which is #{byte_size(data)} bytes long)")

  defp nullify_empty_string(""), do: nil
  defp nullify_empty_string(str) when is_binary(str), do: str

  defp get_rest_from_list_of_fields([_|_] = fields) do
    fields
    |> Enum.reverse()
    |> Enum.drop_while(fn {{_name, val}, _rest} -> is_nil(val) end)
    |> List.first()
    |> elem(1)
  end

  defp fields_to_map(fields) do
    for {name_and_value, _rest} <- fields, into: %{} do
      name_and_value
    end
  end

  defp infer_type(%HashSet{}),               do: :embedded_set
  defp infer_type(%Document{}),              do: :embedded
  defp infer_type(%RID{}),                   do: :link
  defp infer_type(%MarcoPolo.Date{}),        do: :date
  defp infer_type(%MarcoPolo.DateTime{}),    do: :datetime
  defp infer_type(%Decimal{}),               do: :decimal
  defp infer_type(val) when is_boolean(val), do: :boolean
  defp infer_type(val) when is_binary(val),  do: :string
  defp infer_type(val) when is_integer(val), do: :int
  defp infer_type(val) when is_float(val),   do: :double
  defp infer_type(val) when is_list(val),    do: :embedded_list
  defp infer_type(val) when is_map(val),     do: :embedded_map
  defp infer_type(val) when is_nil(val),     do: :boolean # irrelevant
  defp infer_type({type, _}),                do: type

  # http://orientdb.com/docs/last/Types.html
  @types [
    {:boolean, "BOOLEAN", 0},
    {:int, "INTEGER", 1},
    {:short, "SHORT", 2},
    {:long, "LONG", 3},
    {:float, "FLOAT", 4},
    {:double, "DOUBLE", 5},
    {:datetime, "DATETIME", 6},
    {:string, "STRING", 7},
    {:binary, "BINARY", 8},
    {:embedded, "EMBEDDED", 9},
    {:embedded_list, "EMBEDDEDLIST", 10},
    {:embedded_set, "EMBEDDEDSET", 11},
    {:embedded_map, "EMBEDDEDMAP", 12},
    {:link, "LINK", 13},
    {:link_list, "LINKLIST", 14},
    {:link_set, "LINKSET", 15},
    {:link_map, "LINKMAP", 16},
    {:byte, "BYTE", 17},
    {:transient, "TRANSIENT", 18},
    {:date, "DATE", 19},
    {:custom, "CUSTOM", 20},
    {:decimal, "DECIMAL", 21},
    {:link_bag, "LINKBAG", 22},
    {:any, "ANY", 23},
  ]

  for {type_name, stringified_type, type_id} <- @types do
    defp int_to_type(unquote(type_id)),             do: unquote(type_name)
    defp type_to_int(unquote(type_name)),           do: unquote(type_id)
    defp string_to_type(unquote(stringified_type)), do: unquote(type_name)
  end
end
