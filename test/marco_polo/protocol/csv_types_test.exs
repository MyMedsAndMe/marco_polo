defmodule MarcoPolo.Protocol.CSVTypesTest do
  use ExUnit.Case

  import MarcoPolo.Protocol.CSVTypes

  test "decode/1" do
    data = ~s(true,false,88,3.14,_#{Base.encode64 <<1, 2>>}_,,"hi")
    assert decode(data) == [true, false, 88, 3.14, <<1, 2>>, nil, "hi"]
  end
end
