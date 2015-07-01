defmodule MarcoPolo.Protocol.RecordSerializationTest do
  use ExUnit.Case, async: true

  alias MarcoPolo.RID
  alias MarcoPolo.Record
  alias MarcoPolo.DateTime
  alias MarcoPolo.Protocol.RecordSerialization, as: Ser

  import Ser, only: [encode_value: 1, decode_type: 2]

  @record_no_fields <<0,           # version
                      10, "Klass", # class name
                      0,           # end of (empty) header
                      "rest">>

  @record_no_fields_null_class <<0, # version
                                 0, # -1 with zigzag
                                 0, # end of (empty) header
                                 "rest">>

  @record_with_fields <<0,            # version
                        6, "foo",     # class name
                        10, "hello",  # field name
                        0, 0, 0, 26,  # pointer to data
                        7,            # field type (string)
                        6, "int",     # field name
                        0, 0, 0, 34,  # pointer to data
                        1,            # field type (int)
                        0,            # end of header
                        12, "world!", # field value
                        24,           # field value (int with zigzag)
                        "rest">>

  @list <<4,            # number of items (zigzag, hence 2)
          23,           # type of the elems in the list, OrientDB only supports ANY
          7, 8, "elem", # elem type (string) + value
          0, 1,         # elem type (boolean) + value
          "foo">>

  # Throughout these tests, remember that ints are encoded as ZigZag and then as
  # varints (often they're just the double of what they should be because of
  # ZigZag).

  ## Decoding

  test "decode_embedded/1: record with no fields" do
    assert Ser.decode(@record_no_fields) ==
           {%Record{class: "Klass", fields: %{}}, "rest"}
  end

  test "decode_embedded/1: record with no fields and null class" do
    assert Ser.decode(@record_no_fields_null_class) ==
           {%Record{class: nil}, "rest"}
  end

  test "decode_embedded/1: record with fields" do
    record = %Record{class: "foo", fields: %{"hello" => "world!", "int" => 12}}
    assert Ser.decode(@record_with_fields) == {record, "rest"}
  end

  test "decode_type/2: simple types" do
    # booleans
    assert decode_type(<<0, "foo">>, :boolean) == {false, "foo"}
    assert decode_type(<<1, "foo">>, :boolean) == {true, "foo"}

    # integers (with zigzag + varint)
    assert decode_type(<<6, "foo">>, :short) == {3, "foo"}
    assert decode_type(<<44, "foo">>, :int)  == {22, "foo"}
    assert decode_type(<<1, "foo">>, :long)  == {-1, "foo"}

    # floats
    assert {float, "foo"} = decode_type(<<64, 72, 245, 195, "foo">>, :float)
    assert Float.round(float, 2) == 3.14

    # doubles
    assert {float, "foo"} = decode_type(<<64, 9, 30, 184, 81, 235, 133, 31, "foo">>, :double)
    assert Float.round(float, 2) == 3.14

    # strings
    assert decode_type(<<6, "foo", "bar">>, :string) == {"foo", "bar"}

    # binary
    assert decode_type(<<4, 77, 45, "foo">>, :binary) == {<<77, 45>>, "foo"}
  end

  test "decode_type/2: datetime" do
    data = :small_ints.encode_zigzag_varint(1435665809901) <> "foo"
    datetime = %DateTime{year: 2015, month: 6, day: 30,
                         hour: 12, min: 03, sec: 29, msec: 901}
    assert decode_type(data, :datetime) == {datetime, "foo"}
  end

  test "decode_type/2: embedded documents" do
    # Embedded documents have no serialization version
    <<_version, record :: bytes>> = @record_with_fields

    assert decode_type(record, :embedded) ==
           {%Record{class: "foo", fields: %{"hello" => "world!", "int" => 12}}, "rest"}
  end

  test "decode_type/2: embedded lists" do
    assert decode_type(@list, :embedded_list) == {["elem", true], "foo"}
  end

  test "decode_type/2: embedded sets" do
    expected_set = Enum.into(["elem", true], HashSet.new)
    assert decode_type(@list, :embedded_set) == {expected_set, "foo"}
  end

  test "decode_type/2: embedded maps" do
    data = <<4,           # number of keys (zigzag, hence 2)
             7,           # key type (string)
             8, "key1",   # key
             0, 0, 0, 14, # ptr to data
             7,           # data type (string)
             7,           # key type (string)
             8, "key2",   # key
             0, 0, 0, 0,  # ptr to data, 0 means null data
             0,           # when ptr is null type is always 0 (which is boolean, but irrelevant)
             10, "value", # key1 value
             "foo">>

    map = %{"key1" => "value", "key2" => nil}
    assert decode_type(data, :embedded_map) == {map, "foo"}
  end

  test "decode_type/2: links" do
    rid = %RID{cluster_id: 101, position: 99}
    assert decode_type(<<101 :: 32, 99 :: 32, "foo">>, :link) == {rid, "foo"}
  end

  test "decode_type/2: link lists" do
    data = <<4, # number of elements as a zigzag varint
             1 :: 32, 2 :: 32, # link
             8 :: 32, 9 :: 32, # link
             "foo">>

    list = {:link_list, [%RID{cluster_id: 1, position: 2}, %RID{cluster_id: 8, position: 9}]}
    assert decode_type(data, :link_list) == {list, "foo"}
  end

  test "decode_type/2: link sets" do
    data = <<4, # number of elements as a zigzag varint
             0 :: 32, 1 :: 32, # link
             9 :: 32, 9 :: 32, # link
             "foo">>

    links = [%RID{cluster_id: 9, position: 9}, %RID{cluster_id: 0, position: 1}]
    expected_set   = Enum.into(links, HashSet.new)
    assert {{:link_set, set}, "foo"} = Ser.decode_type(data, :link_set)
    assert Set.equal?(expected_set, set)
  end

  test "decode_type/2: decimals" do
    Decimal.set_context(%Decimal.Context{precision: 5})

    data = <<0, 0, 0, 4,   # scale as...4 bytes? why? :(
             0, 0, 0, 2,   # length of the value bytes as...4 bytes :(
             <<122, 183>>, # value (31415)
             "foo">>

    assert decode_type(data, :decimal) == {Decimal.new(3.1415), "foo"}
  end

  ## Encoding

  test "encode_value/1: simple types" do
    # booleans
    assert bin(encode_value(true)) == <<1>>
    assert bin(encode_value(false)) == <<0>>

    # strings and binaries
    assert bin(encode_value("foo"))         == <<6, "foo">>
    assert bin(encode_value(<<0, 100, 1>>)) == <<6, 0, 100, 1>>

    # ints
    assert bin(encode_value(1)) == <<2>>
    assert bin(encode_value(1010)) == :small_ints.encode_zigzag_varint(1010)
    assert bin(encode_value(123456)) == :small_ints.encode_zigzag_varint(123456)

    # floats and doubles
    # (Elixir floats are always encoded as OrientDB doubles - 8 bytes)
    assert bin(encode_value(3.14)) == <<64, 9, 30, 184, 81, 235, 133, 31>>
    assert bin(encode_value({:float, 3.14})) == <<64, 72, 245, 195>>
    assert bin(encode_value({:double, 3.14})) == <<64, 9, 30, 184, 81, 235, 133, 31>>
  end

  test "encode_value/1: datetime" do
    datetime = %DateTime{year: 2015, month: 6, day: 30,
                         hour: 12, min: 03, sec: 29, msec: 901}
    assert bin(encode_value(datetime)) == :small_ints.encode_zigzag_varint(1435665809901)
  end

  test "encode_value/1: embedded document" do
    record = %Record{class: "Klass"}
    <<_version, record_content :: binary>> = @record_no_fields
    assert bin(encode_value(record)) <> "rest" == record_content
  end

  test "encode_value/1: embedded lists" do
    assert (bin(encode_value(["elem", true])) <> "foo") == @list
  end

  test "encode_value/1: embedded sets" do
    expected = <<4,            # number of items (zigzag, hence 2)
                 23,           # type of the elems in the list, OrientDB only supports ANY
                 0, 1,         # elem type (boolean) + value
                 7, 8, "elem", # elem type (string) + value
                 >>

    set = Enum.into([true, "elem"], HashSet.new)
    assert bin(encode_value(set)) == expected
  end

  defp bin(iodata) do
    IO.iodata_to_binary(iodata)
  end
end
