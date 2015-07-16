defmodule MarcoPolo.Protocol.CSVTypesTest do
  use ExUnit.Case

  doctest MarcoPolo.Protocol.CSVTypes

  import MarcoPolo.Protocol.CSVTypes

  test "decode/1: booleans" do
    assert decode("true") == true
    assert decode("false") == false
  end

  test "decode/1: strings" do
    data = ~s("hey there")
    assert decode(data) == "hey there"
  end

  test "decode/1: binaries" do
    data = "_" <> Base.encode64(<<1, 2, 3>>) <> "_"
    assert decode(data) == <<1, 2, 3>>
  end

  test "decode/1: nil" do
    assert decode("") == nil
  end

  test "decode/1: integers" do
    assert decode("93") == 93
    assert decode("-93") == -93
    assert decode("054s") == 54
    assert decode("-054s") == -54
    assert decode("1234567890l") == 1234567890
    assert decode("-1234567890l") == -1234567890
  end

  test "decode/1: floats" do
    assert decode("2.71f") == 2.71
    assert decode("-2.71f") == -2.71
    assert decode("1.01d") == 1.01
    assert decode("-1.01d") == -1.01
  end

  test "decode/1: decimals" do
    assert decode("3.14c") == Decimal.new(1, 314, -2)
    assert decode("-3.14c") == Decimal.new(-1, 314, -2)
  end

  test "decode/1: dates" do
    assert decode("1436983328000a") == %MarcoPolo.Date{day: 15, month: 7, year: 2015}
  end

  test "decode/1: datetimes" do
    assert decode("1436983328032t") == %MarcoPolo.DateTime{day: 15, month: 7, year: 2015,
                                                           hour: 18, min: 2, sec: 8, msec: 32}
  end

  test "decode/1: RIDs" do
    assert decode("#0:1") == %MarcoPolo.RID{cluster_id: 0, position: 1}
    assert decode("#10:232") == %MarcoPolo.RID{cluster_id: 10, position: 232}
  end
end
