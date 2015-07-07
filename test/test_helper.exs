ExUnit.start()

clusters = []
records  = []

run_script = fn(script) ->
  case System.cmd("orientdb-console", [script]) do
    {lines, 0}   -> lines
    {_, _status} -> raise "Database setup in test/test_helper.exs failed"
  end
end

extract_rid = fn(str) ->
  %{"cid" => cluster_id, "pos" => position} =
    Regex.named_captures ~r/Inserted record.*(#(?<cid>\d+):(?<pos>\d+))/, str

  %MarcoPolo.RID{
    cluster_id: String.to_integer(cluster_id),
    position: String.to_integer(position),
  }
end

extract_cluster_id = fn(str) ->
  %{"id" => cluster_id} =
    Regex.named_captures(~r/Cluster created correctly with id #(?<id>\d+)/, str)

  String.to_integer cluster_id
end

insert_record = fn(name, insert_cmd) ->
  output = run_script.("""
  CONNECT remote:localhost/MarcoPoloTest admin admin;
  #{insert_cmd}
  """)

  {name, extract_rid.(output)}
end

run_script.("""
SET ignoreErrors true;
DROP DATABASE remote:localhost/MarcoPoloTest root root;
DROP DATABASE remote:localhost/MarcoPoloTestGenerated root root;
DROP DATABASE remote:localhost/MarcoPoloToDrop root root;
SET ignoreErrors false;

CREATE DATABASE remote:localhost/MarcoPoloTest root root plocal;
CREATE DATABASE remote:localhost/MarcoPoloToDrop root root memory;
""")

output = run_script.("""
CONNECT remote:localhost/MarcoPoloTest admin admin;
CREATE CLUSTER schemaless;
CREATE CLASS Schemaless CLUSTER schemaless;
""")

clusters = [{"schemaless", extract_cluster_id.(output)}|clusters]

output = run_script.("""
CONNECT remote:localhost/MarcoPoloTest admin admin;
CREATE CLUSTER schemaful;
CREATE CLASS Schemaful;
CREATE PROPERTY Schemaful.myString STRING;
""")

clusters = [{"schemaful", extract_cluster_id.(output)}|clusters]

# Insert some records
records = records ++ [
  insert_record.("record_delete", "INSERT INTO Schemaless(name) VALUES ('record_delete');"),
  insert_record.("record_update", "INSERT INTO Schemaless(name, f) VALUES ('record_update', 'foo');"),
  insert_record.("record_load", "INSERT INTO Schemaless(name) VALUES ('record_load');"),
  insert_record.("schemaless_record_load", "INSERT INTO Schemaful(myString) VALUES ('record_load');"),
]

defmodule TestHelpers do
  def user,     do: System.get_env("ORIENTDB_USER")
  def password, do: System.get_env("ORIENTDB_PASS")

  def cluster_id(name) do
    {_, id} = List.keyfind(unquote(Macro.escape(clusters)), name, 0)
    id
  end

  def record_rid(name) do
    {_, rid} = List.keyfind(unquote(Macro.escape(records)), name, 0)
    rid
  end
end
