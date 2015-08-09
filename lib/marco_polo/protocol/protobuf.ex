defmodule MarcoPolo.Protocol.Protobuf do
  @moduledoc false

  require Bitwise

  @doc """
  Decodes a varint and then decodes that varint with ZigZag.
  """
  @spec decode_zigzag_varint(binary) :: {integer, binary}
  def decode_zigzag_varint(data) do
    {i, rest} = decode_varint(data)
    {decode_zigzag(i), rest}
  end

  @doc """
  Encodes an int with ZigZag and then encodes the result with varint.
  """
  @spec encode_zigzag_varint(integer) :: binary
  def encode_zigzag_varint(i) do
    i |> encode_zigzag |> encode_varint
  end

  defp decode_varint(data) when is_binary(data) do
    decode_varint(data, 0, 0)
  end

  defp decode_varint(<<1 :: 1, number :: 7, rest :: binary>>, position, acc) do
    decode_varint(rest, position + 7, Bitwise.bsl(number, position) + acc)
  end

  defp decode_varint(<<0 :: 1, number :: 7, rest :: binary>>, position, acc) do
    {Bitwise.bsl(number, position) + acc, rest}
  end

  defp encode_varint(i) when i >= 0 and i <= 127,
    do: <<i>>
  defp encode_varint(i) when i > 127,
    do: <<1 :: 1, Bitwise.band(i, 127) :: 7, encode_varint(Bitwise.bsr(i, 7)) :: binary>>


  defp decode_zigzag(i) when i >= 0 and rem(i, 2) == 0,
    do: div(i, 2)
  defp decode_zigzag(i) when i >= 0 and rem(i, 2) == 1,
    do: - div(i, 2) - 1

  defp encode_zigzag(i) when i >= 0, do: i * 2
  defp encode_zigzag(i) when i < 0, do: - (i * 2) - 1
end
