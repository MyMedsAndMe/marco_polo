defmodule MarcoPolo.Binary.Encoder do
  import MarcoPolo.Binary.Helpers

  def encode(true),
    do: <<1>>
  def encode(false),
    do: <<0>>

  def encode(nil),
    do: encode({:int, -1})

  def encode(str) when is_binary(str),
    do: encode({:int, byte_size(str)}) <> str

  def encode({:raw, bytes}) when is_binary(bytes),
    do: bytes

  # Encoding an Elixir integer defaults to encoding an OrientDB int (4 bytes).
  def encode(i) when is_integer(i),
    do: encode({:int, i})

  def encode({:short, i}),
    do: <<i :: short>>
  def encode({:int, i}),
    do: <<i :: int>>
  def encode({:long, i}),
    do: <<i :: long>>
end
