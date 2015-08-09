defmodule MarcoPolo.Protocol.TypesTest do
  use ExUnit.Case, async: true

  import MarcoPolo.Protocol.BinaryHelpers
  import MarcoPolo.Protocol.Types

  test "encode/1: booleans" do
    assert encode(true)  == <<1>>
    assert encode(false) == <<0>>
  end

  test "encode/1: nil is serialized as bytes of length -1" do
    assert encode(nil) == <<-1 :: int>>
  end

  test "encode/1: shorts" do
    assert encode({:short, 34})  == <<34 :: short>>
    assert encode({:short, -11}) == <<-11 :: short>>
  end

  test "encode/1: ints" do
    assert encode({:int, 2931})   == <<2931 :: int>>
    assert encode({:int, -85859}) == <<-85859 :: int>>
  end

  test "encode/1: longs" do
    assert encode({:long, 1234567890})  == <<1234567890 :: long>>
    assert encode({:long, -1234567890}) == <<-1234567890 :: long>>
  end

  test "encode/1: Elixir integers are serialized as ints by default" do
    assert encode(1)   == <<1 :: int>>
    assert encode(-42) == <<-42 :: int>>
  end

  test "encode/1: strings" do
    assert encode("foo") == <<3 :: int, "foo">>
    assert encode("føø") == <<byte_size("føø") :: int, "føø">>
  end

  test "encode/1: sequences of bytes" do
    assert IO.iodata_to_binary(encode(<<1, 2, 3>>)) == <<3 :: int, 1, 2, 3>>
  end

  test "encode/1: raw bytes" do
    assert IO.iodata_to_binary(encode({:raw, "foo"})) == "foo"
  end

  test "encode/1: iolists" do
    assert IO.iodata_to_binary(encode([?f, [?o, "o"]])) == <<3 :: int, "foo">>
  end

  test "encode/1: binary records" do
    rec = %MarcoPolo.BinaryRecord{contents: <<100, 2, 93>>}
    assert encode(rec) == <<3 :: 32, 100, 2, 93>>
  end

  test "encode_list/1" do
    terms    = [{:short, 3}, {:raw, <<1>>}, "foo", false]
    expected = <<3 :: 16, 1, 3 :: 32, "foo", 0>>
    assert IO.iodata_to_binary(encode_list(terms)) == expected
  end

  test "decode/2: string and bytes" do
    assert decode(<<-1 :: int, "foo">>, :string) == {nil, "foo"}
    assert decode(<<-1 :: int, "foo">>, :bytes) == {nil, "foo"}

    assert decode(<<3 :: int, "foo", "rest">>, :string) == {"foo", "rest"}
    assert decode(<<2 :: int, <<8, 7>>, "rest">>, :bytes) == {<<8, 7>>, "rest"}

    assert decode(<<2 :: int, "a">>, :string) == :incomplete
    assert decode(<<1 :: int>>, :bytes) == :incomplete
  end

  test "decode/2: single bytes" do
    assert decode(<<?k, "rest">>, :byte) == {?k, "rest"}
    assert decode(<<>>, :byte) == :incomplete
  end

  test "decode/2: ints, longs and shorts" do
    assert decode(<<44 :: int, "rest">>, :int)     == {44, "rest"}
    assert decode(<<44 :: short, "rest">>, :short) == {44, "rest"}
    assert decode(<<44 :: long, "rest">>, :long)   == {44, "rest"}

    assert decode(<<44 :: 24>>, :int) == :incomplete
  end
end
