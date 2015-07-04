defmodule MarcoPolo.QueryParserTest do
  use ExUnit.Case, async: true

  import MarcoPolo.QueryParser

  test "query_type/1" do
    assert query_type("SELECT FROM Foo") == :sql_query
    assert query_type("CREATE CLASS MyClass") == :sql_command
    assert query_type("ALTERCLASS MyClass ADDCLUSTER foo") == :sql_command
  end
end
