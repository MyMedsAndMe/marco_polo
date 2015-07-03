ExUnit.start()

db_url = "remote:localhost/MarcoPoloTestDb"

run_script = fn(script) ->
  case System.cmd("orientdb-console", [script]) do
    {lines, 0} ->
      lines
    {_, status} ->
      raise """
      Database setup in test/test_helper.exs failed with exit status: #{inspect status}
      """
  end
end

output = run_script.("""
SET ignoreErrors true;
DROP DATABASE #{db_url} root root;
SET ignoreErrors false;
CREATE DATABASE #{db_url} root root plocal;
CONNECT #{db_url} admin admin;
CREATE CLUSTER schemaless;
CREATE CLASS Schemaless CLUSTER schemaless;
""")

%{"id" => id} = Regex.named_captures(~r/Cluster created correctly with id #(?<id>\d+)/, output)

defmodule TestHelpers do
  def cluster_id, do: unquote(String.to_integer(id))

  def user,     do: System.get_env("ORIENTDB_USER")
  def password, do: System.get_env("ORIENTDB_PASS")
end
