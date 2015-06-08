defmodule MarcoPolo.ProtocolTest do
  use ExUnit.Case, async: true

  import MarcoPolo.Protocol.BinaryHelpers
  alias MarcoPolo.Protocol

  test "serialize/1: booleans" do
    assert Protocol.serialize(true)  == <<1>>
    assert Protocol.serialize(false) == <<0>>
  end

  test "serialize/1: nil is serialized as bytes of length -1" do
    assert Protocol.serialize(nil) == <<-1 :: int>>
  end

  test "serialize/1: shorts" do
    assert Protocol.serialize({:short, 34})  == <<34 :: short>>
    assert Protocol.serialize({:short, -11}) == <<-11 :: short>>
  end

  test "serialize/1: ints" do
    assert Protocol.serialize({:int, 2931})   == <<2931 :: int>>
    assert Protocol.serialize({:int, -85859}) == <<-85859 :: int>>
  end

  test "serialize/1: longs" do
    assert Protocol.serialize({:long, 1234567890})  == <<1234567890 :: long>>
    assert Protocol.serialize({:long, -1234567890}) == <<-1234567890 :: long>>
  end

  test "serialize/1: Elixir integers are serialized as ints by default" do
    assert Protocol.serialize(1)   == <<1 :: int>>
    assert Protocol.serialize(-42) == <<-42 :: int>>
  end

  test "serialize/1: strings" do
    assert Protocol.serialize("foo") == <<3 :: int, "foo">>
    assert Protocol.serialize("føø") == <<byte_size("føø") :: int, "føø">>
  end

  test "serialize/1: sequences of bytes" do
    assert Protocol.serialize(<<1, 2, 3>>) == <<3 :: int, 1, 2, 3>>
  end
end
