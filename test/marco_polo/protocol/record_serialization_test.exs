defmodule MarcoPolo.Protocol.RecordSerializationTest do
  use ExUnit.Case, async: true

  import MarcoPolo.Protocol.RecordSerialization, only: [encode_type: 2]

  # Throughout these tests, remember that ints are encoded as ZigZag and then as
  # varints (often they're just the double of what they should be because of
  # ZigZag).

  test "encode_type/2: strings" do
    assert bin(encode_type("foo", :string)) == <<6, "foo">>
  end

  test "encode_type/2: ints" do
    assert bin(encode_type(1, :sint16)) == <<2>>
    assert bin(encode_type(1010, :sint32)) == :small_ints.encode_zigzag_varint(1010)
    assert bin(encode_type(123456, :sint64)) == :small_ints.encode_zigzag_varint(123456)
  end

  defp bin(iodata) do
    IO.iodata_to_binary(iodata)
  end
end
