defmodule MarcoPolo.Binary.ProtocolTest do
  use ExUnit.Case

  import MarcoPolo.Binary.Helpers
  import MarcoPolo.Binary.Protocol

  test "implementation for booleans" do
    assert encode(true) == <<1>>
    assert encode(false) == <<0>>
  end

  test "the implementation for atoms only works for true and false" do
    msg = "only atoms true and false can be encoded"
    assert_raise ArgumentError, msg, fn -> encode(:foo) end
    assert_raise ArgumentError, msg, fn -> encode(nil) end
  end

  test "implementation for shorts" do
    assert encode(34, :short) == <<34 :: short>>
    assert encode(-11, :short) == <<-11 :: short>>
  end

  test "implementation for ints" do
    assert encode(2931, :int) == <<2931 :: int>>
    assert encode(-85859, :int) == <<-85859 :: int>>
  end

  test "implementation for longs" do
    assert encode(1234567890, :long) == <<1234567890 :: long>>
    assert encode(-1234567890, :long) == <<-1234567890 :: long>>
  end

  test "implementation for strings" do
    assert encode("foo") == <<3 :: int, "foo">>
    assert encode("føø") == <<byte_size("føø") :: int, "føø">>
  end

  test "implementation for sequences of bytes" do
    assert encode(<<1, 2, 3>>) == <<3 :: int, 1, 2, 3>>
  end
end
