defmodule MarcoPolo.Binary.Decoder do
  @moduledoc false

  import MarcoPolo.Binary.Helpers

  # Basic data types.

  def decode(<<length :: int, rest :: binary>>, :string) do
    byte_length = bytes(length)
    <<string :: bits-size(byte_length), rest :: binary>> = rest
    {string, rest}
  end

  def decode(data, :bytes) do
    decode(data, :string)
  end

  def decode(<<i :: short, rest :: binary>>, :short) do
    {i, rest}
  end

  def decode(<<i :: int, rest :: binary>>, :int) do
    {i, rest}
  end

  def decode(<<i :: long, rest :: binary>>, :long) do
    {i, rest}
  end

  def decode(<<0, rest :: binary>>, :boolean) do
    {false, rest}
  end

  def decode(<<_byte, rest :: binary>>, :boolean) do
    {true, rest}
  end

  def decode(data, {:raw, n}) do
    len = bytes(n)
    <<raw :: bits-size(len), rest :: binary>> = data
    {raw, rest}
  end

  # "Complex" data types.

  def decode_multiple(data, types) do
    Enum.map_reduce types, data, &decode(&2, &1)
  end

  def decode_array(data, n, types) do
    do_decode_array(data, n, types, [])
  end

  def do_decode_array(data, 0, types, acc) do
    {Enum.reverse(acc), data}
  end

  def do_decode_array(data, n, types, acc) do
    {arr, rest} = decode_multiple(data, types)
    do_decode_array(rest, n - 1, types, [arr|acc])
  end
end
