defmodule MarcoPolo.Binary.ParserTest do
  use ExUnit.Case, async: true
  import MarcoPolo.Binary.Decoder
  import MarcoPolo.Binary.Helpers

  test "decode/2: regular string" do
    assert decode(<<3 :: int, "foo", "rest">>, :string) == {"foo", "rest"}
  end

  test "decode/2: empty string" do
    assert decode(<<0 :: int, "">>, :string) == {"", <<>>}
  end

  test "decode/2: string with unicode characters" do
    str = "føø bå® ♫"
    assert decode(<<byte_size(str) :: int, str :: bytes>>, :string) == {str, <<>>}
  end

  test "decode/2: shorts" do
    assert decode(<<2 :: short, "foo">>, :short) == {2, "foo"}
    assert decode(<<-2 :: short>>, :short) == {-2, ""}
  end

  test "decode/2: ints" do
    assert decode(<<384 :: int, "foo">>, :int) == {384, "foo"}
    assert decode(<<-384 :: int>>, :int) == {-384, ""}
  end

  test "decode/2: longs" do
    assert decode(<<1234567890 :: long, "foo">>, :long) == {1234567890, "foo"}
    assert decode(<<-1234567890 :: long>>, :long) == {-1234567890, ""}
  end

  test "decode/2: booleans" do
    assert decode(<<7, "foo">>, :boolean) == {true, "foo"}
    assert decode(<<1>>, :boolean) == {true, ""}
    assert decode(<<0, "føø">>, :boolean) == {false, "føø"}
  end

  test "decode_multiple/2: straightforward use case" do
    bytes = <<1, 0, 3 :: int, "foo", 1000 :: long, "rest">>
    assert decode_multiple(bytes, [:boolean, :boolean, :string, :long])
           == {[true, false, "foo", 1000], "rest"}
  end

  test "decode_multiple/2: supports raw bytes" do
    bytes = <<7, 10 :: short, "raw", 1 :: int, "rest">>
    assert decode_multiple(bytes, [:boolean, :short, {:raw, 3}, :int])
           == {[true, 10, "raw", 1], "rest"}
  end

  test "decode_array/3: empty array" do
    assert decode_array(<<"foo">>, 0, [:boolean, :int, :string, :long])
           == {[], "foo"}
  end

  test "decode_array/3: non-empty array" do
    bytes = <<0, "foo", 3 :: int, "bar", 1, "baz", 4 :: int, "bong", "rest">>
    assert decode_array(bytes, 2, [:boolean, {:raw, 3}, :string])
           {[[false, "foo", "bar"], [true, "baz", "bong"]], "rest"}
  end
end
