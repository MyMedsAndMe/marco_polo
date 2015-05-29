defmodule MarcoPolo.Binary.EncoderTest do
  use ExUnit.Case, async: true

  import MarcoPolo.Binary.Helpers

  import MarcoPolo.Binary.Encoder, only: [encode: 1]

  test "encode/1: booleans" do
    assert encode(true) == <<1>>
    assert encode(false) == <<0>>
  end

  test "encode/1: nil is encoded as bytes of length -1" do
    assert encode(nil) == <<-1 :: int>>
  end

  test "encode/1: shorts" do
    assert encode({:short, 34}) == <<34 :: short>>
    assert encode({:short, -11}) == <<-11 :: short>>
  end

  test "encode/1: ints" do
    assert encode({:int, 2931}) == <<2931 :: int>>
    assert encode({:int, -85859}) == <<-85859 :: int>>
  end

  test "encode/1: longs" do
    assert encode({:long, 1234567890}) == <<1234567890 :: long>>
    assert encode({:long, -1234567890}) == <<-1234567890 :: long>>
  end

  test "encode/1: Elixir integers are encoded as ints by default" do
    assert encode(1) == <<1 :: int>>
    assert encode(-42) == <<-42 :: int>>
  end

  test "encode/1: strings" do
    assert encode("foo") == <<3 :: int, "foo">>
    assert encode("føø") == <<byte_size("føø") :: int, "føø">>
  end

  test "encode/1: sequences of bytes" do
    assert encode(<<1, 2, 3>>) == <<3 :: int, 1, 2, 3>>
  end

  test "encode/1: raw bytes" do
    assert encode({:raw, <<1, 2, 3>>}) == <<1, 2, 3>>
  end
end
