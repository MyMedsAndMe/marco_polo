defmodule MarcoPolo.ProtocolTest do
  use ExUnit.Case, async: true

  alias MarcoPolo.Protocol
  alias MarcoPolo.Error

  import MarcoPolo.Protocol.BinaryHelpers

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
