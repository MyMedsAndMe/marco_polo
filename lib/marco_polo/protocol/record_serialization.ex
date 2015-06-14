defmodule MarcoPolo.Protocol.RecordSerialization do
  import Record
  defrecordp :property, [:id, :data_ptr]
  defrecordp :named_field, [:name, :data_type, :data_ptr]

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

  defp decode_zigzag(i) when rem(i, 2) == 0, do: div(i, 2)
  defp decode_zigzag(i) when rem(i, 2) == 1, do: - (div(i, 2) + 1)

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

  defp decode_header_fields(data, acc \\ [])

  defp decode_header_fields(<<0, _data :: binary>>, acc) do
    Enum.reverse(acc)
  end

  defp decode_header_fields(data, acc) do
    {i, rest} = decode_zigzag_varint(data)

    if i < 0 do
      {data_ptr, rest} = decode_data_ptr(rest)
      property = property(id: decode_property_id(i), data_ptr: data_ptr)
      decode_header_fields(rest, [property|acc])
    else
      {field_name, rest}            = decode_string(data)
      {data_ptr, rest}              = decode_data_ptr(rest)
      <<data_type, rest :: binary>> = rest
      field = named_field(name: field_name, data_type: int_to_type(data_type), data_ptr: data_ptr)
      decode_header_fields(rest, [field|acc])
    end
  end

  defp decode_data_ptr(data) do
    <<data_ptr :: 32-signed, rest :: binary>> = data
    {data_ptr, rest}
  end

  defp decode_property_id(i) do
    (i * -1) - 1
  end

  defp decode_field(data, field) when is_record(field, :named_field) do
    named_field(data_ptr: ptr, data_type: type) = field
    {_, field_start} = :erlang.split_binary(data, ptr)
    value = decode_type(field_start, type)
    value
  end

  defp decode_type(<<0>> <> _, :boolean), do: false
  defp decode_type(<<1>> <> _, :boolean), do: false

  defp decode_type(data, type) when type in [:sint16, :sint32, :sint64] do
    {i, _} = decode_zigzag_varint(data)
    i
  end

  defp decode_type(data, :string) do
    {string, _} = decode_string(data)
    string
  end

  defp field_name(field) when is_record(field, :named_field) do
    named_field(field, :name)
  end

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
