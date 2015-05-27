defmodule MarcoPolo.Binary.ParserTest do
  use ExUnit.Case, async: true
  import MarcoPolo.Binary.Parser
  import MarcoPolo.Binary.Helpers

  test "parse_string/1: regular string" do
    assert parse_string(<<3 :: bytes(4), "foo", "rest">>) == {"foo", "rest"}
  end

  test "parse_string/1: empty string" do
    assert parse_string(<<0 :: bytes(4), "">>) == {"", <<>>}
  end

  test "parse_string/1: unicode characters" do
    str = "føø bå® ♫"
    assert parse_string(<<byte_size(str) :: bytes(4), str :: bytes>>) == {str, <<>>}
  end

  test "parse_short/1" do
    assert parse_short(<<2 :: bytes(2), "foo">>) == {2, "foo"}
    assert parse_short(<<-2 :: bytes(2)>>) == {-2, ""}
  end

  test "parse_int/1" do
    assert parse_int(<<384 :: bytes(4), "foo">>) == {384, "foo"}
    assert parse_int(<<-384 :: bytes(4)>>) == {-384, ""}
  end

  test "parse_long/1" do
    assert parse_long(<<1234567890 :: bytes(8), "foo">>) == {1234567890, "foo"}
    assert parse_long(<<-1234567890 :: bytes(8)>>) == {-1234567890, ""}
  end

  test "parse_bool/1" do
    assert parse_boolean(<<7, "foo">>) == {true, "foo"}
    assert parse_boolean(<<1>>) == {true, ""}
    assert parse_boolean(<<0, "føø">>) == {false, "føø"}
  end
end
