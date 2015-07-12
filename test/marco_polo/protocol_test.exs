defmodule MarcoPolo.ProtocolTest do
  use ExUnit.Case, async: true

  import MarcoPolo.Protocol.BinaryHelpers
  alias MarcoPolo.Protocol
  alias MarcoPolo.Error

  test "encode_term/1: booleans" do
    assert Protocol.encode_term(true)  == <<1>>
    assert Protocol.encode_term(false) == <<0>>
  end

  test "encode_term/1: nil is serialized as bytes of length -1" do
    assert Protocol.encode_term(nil) == <<-1 :: int>>
  end

  test "encode_term/1: shorts" do
    assert Protocol.encode_term({:short, 34})  == <<34 :: short>>
    assert Protocol.encode_term({:short, -11}) == <<-11 :: short>>
  end

  test "encode_term/1: ints" do
    assert Protocol.encode_term({:int, 2931})   == <<2931 :: int>>
    assert Protocol.encode_term({:int, -85859}) == <<-85859 :: int>>
  end

  test "encode_term/1: longs" do
    assert Protocol.encode_term({:long, 1234567890})  == <<1234567890 :: long>>
    assert Protocol.encode_term({:long, -1234567890}) == <<-1234567890 :: long>>
  end

  test "encode_term/1: Elixir integers are serialized as ints by default" do
    assert Protocol.encode_term(1)   == <<1 :: int>>
    assert Protocol.encode_term(-42) == <<-42 :: int>>
  end

  test "encode_term/1: strings" do
    assert Protocol.encode_term("foo") == <<3 :: int, "foo">>
    assert Protocol.encode_term("føø") == <<byte_size("føø") :: int, "føø">>
  end

  test "encode_term/1: sequences of bytes" do
    assert IO.iodata_to_binary(Protocol.encode_term(<<1, 2, 3>>)) == <<3 :: int, 1, 2, 3>>
  end

  test "encode_term/1: raw bytes" do
    assert IO.iodata_to_binary(Protocol.encode_term({:raw, "foo"})) == "foo"
  end

  test "encode_term/1: iolists" do
    assert IO.iodata_to_binary(Protocol.encode_term([?f, [?o, "o"]])) == <<3 :: int, "foo">>
  end

  test "encode_list_of_terms/1" do
    terms    = [{:short, 3}, {:raw, <<1>>}, "foo", false]
    expected = <<3 :: 16, 1, 3 :: 32, "foo", 0>>
    assert IO.iodata_to_binary(Protocol.encode_list_of_terms(terms)) == expected
  end

  test "decode_term/2: string and bytes" do
    assert Protocol.decode_term(<<-1 :: int, "foo">>, :string) == {nil, "foo"}
    assert Protocol.decode_term(<<-1 :: int, "foo">>, :bytes) == {nil, "foo"}

    assert Protocol.decode_term(<<3 :: int, "foo", "rest">>, :string) == {"foo", "rest"}
    assert Protocol.decode_term(<<2 :: int, <<8, 7>>, "rest">>, :bytes) == {<<8, 7>>, "rest"}

    assert Protocol.decode_term(<<2 :: int, "a">>, :string) == :incomplete
    assert Protocol.decode_term(<<1 :: int>>, :bytes) == :incomplete
  end

  test "decode_term/2: single bytes" do
    assert Protocol.decode_term(<<?k, "rest">>, :byte) == {?k, "rest"}
    assert Protocol.decode_term(<<>>, :byte) == :incomplete
  end

  test "decode_term/2: ints, longs and shorts" do
    assert Protocol.decode_term(<<44 :: int, "rest">>, :int)     == {44, "rest"}
    assert Protocol.decode_term(<<44 :: short, "rest">>, :short) == {44, "rest"}
    assert Protocol.decode_term(<<44 :: long, "rest">>, :long)   == {44, "rest"}

    assert Protocol.decode_term(<<44 :: 24>>, :int) == :incomplete
  end

  test "parse_resp/3: parsing error responses" do
    data = <<1,                                 # error response
             10 :: int,                         # sid
             1,                                 # there are still errors
             3 :: int, "foo", 3 :: int, "bar",  # class + message
             1,                                 # there are still errors
             3 :: int, "baz", 4 :: int, "bong", # class + message
             0,                                 # no more errors
             3 :: int, <<1, 2, 3>>,             # binary dump of the exception
             "rest">>


    assert {_sid, {:error, %Error{} = err}, "rest"} = Protocol.parse_resp(:foo, data, %{})
    assert err.errors == [{"foo", "bar"}, {"baz", "bong"}]
  end
end
