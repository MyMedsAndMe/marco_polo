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

  @doc """
  Parses a binary-serialized record.
  """
  @spec decode(binary) :: {non_neg_integer, String.t, %{}}
  def decode(data) do
    <<version, rest :: binary>> = data
    {class_name, rest} = decode_string(rest)
    fields = decode_header_fields(rest)

    fields = Enum.reduce fields, %{}, fn(field, acc) ->
      value = decode_field(data, field)
      Map.put(acc, field_name(field), value)
    end

    {version, class_name, fields}
  end

  # From Google's Protocol Buffer:
  #
  #   ZigZag encoding maps signed integers to unsigned integers so that numbers
  #   with a small absolute value (for instance, -1) have a small varint encoded
  #   value too. It does this in a way that "zig-zags" back and forth through the
  #   positive and negative integers, so that -1 is encoded as 1, 1 is encoded as
  #   2, -2 is encoded as 3, and so on [...].
  #
  defp decode_zigzag(i) when rem(i, 2) == 0, do: div(i, 2)
  defp decode_zigzag(i) when rem(i, 2) == 1, do: - (div(i, 2) + 1)

  # varints are decoded using the `:gpb` Erlang library (which is a dependency
  # of this project).
  defp decode_zigzag_varint(data) when is_binary(data) do
    {i, rest} = :gpb.decode_varint(data)
    {decode_zigzag(i), rest}
  end

  defp decode_string(data) do
    {len, rest} = decode_zigzag_varint(data)
    len = len * 8
    <<string :: bits-size(len), rest :: binary>> = rest
    {string, rest}
  end

  defp decode_header_fields(data, acc \\ []) do
    {i, rest} = decode_zigzag_varint(data)

    # If `i` is positive, that means the next field definition is a "named
    # field" and `i` is the length of the field's name. If it's negative, it
    # represents the (weirdly encoded) property id of a property. If it's 0, it
    # signals the end of the header segment.

    cond do
      i == 0 ->
        Enum.reverse(acc)
      i < 0 ->
        {data_ptr, rest} = decode_data_ptr(rest)
        property = property(id: decode_property_id(i), data_ptr: data_ptr)
        decode_header_fields(rest, [property|acc])
      i > 0 ->
        {field_name, rest}            = decode_string(data)
        {data_ptr, rest}              = decode_data_ptr(rest)
        <<data_type, rest :: binary>> = rest
        field = named_field(name: field_name, data_type: int_to_type(data_type), data_ptr: data_ptr)
        decode_header_fields(rest, [field|acc])
    end
  end

  # The pointer to the data is just a signed int32.
  defp decode_data_ptr(data) do
    <<data_ptr :: 32-signed, rest :: binary>> = data
    {data_ptr, rest}
  end

  # Don't ask why.
  defp decode_property_id(i) do
    (i * -1) - 1
  end

  # Decodes a field given the `field` definition and the whole `data`. Returns
  # just the value (not the usual `{value, rest}` tuple) as `field` contains a
  # pointer to its data in `data`.
  defp decode_field(data, field) when is_record(field, :named_field) do
    named_field(data_ptr: ptr, data_type: type) = field
    {_, field_start} = :erlang.split_binary(data, ptr)
    decode_type(field_start, type)
  end

  defp decode_type(<<0>> <> _, :boolean), do: false
  defp decode_type(<<1>> <> _, :boolean), do: false

  defp decode_type(data, type) when type in [:sint16, :sint32, :sint64] do
    decode_zigzag_varint(data) |> elem(1)
  end

  defp decode_type(data, :string) do
    decode_string(data) |> elem(1)
  end

  defp field_name(field) when is_record(field, :named_field) do
    named_field(field, :name)
  end

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
