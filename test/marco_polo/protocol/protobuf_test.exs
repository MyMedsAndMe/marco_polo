defmodule MarcoPolo.Protocol.ProtobufTest do
  use ExUnit.Case, async: true

  import MarcoPolo.Protocol.Protobuf

  test "decode_zigzag_varint/1" do
    assert decode_zigzag_varint(<<4, "foo">>) == {2, "foo"}
    assert decode_zigzag_varint(<<44034 :: 16, "bar">>) == {150, "bar"}
  end

  test "encode_zigzag_varint/1" do
    assert encode_zigzag_varint(-9) == <<17>>
    assert encode_zigzag_varint(150) == <<44034 :: 16>>
  end
end
