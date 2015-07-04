ExUnit.start()

db_name = "MarcoPoloTestDb"
db_url  = "remote:localhost/" <> db_name

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

insert_record = fn(name, insert_cmd) ->
  output = run_script.("""
  CONNECT #{db_url} admin admin;
  #{insert_cmd}
  """)

  {name, extract_rid.(output)}
end

output = run_script.("""
SET ignoreErrors true;
DROP DATABASE #{db_url} root root;
DROP DATABASE remote:localhost/TestGeneratedDb root root;
SET ignoreErrors false;

CREATE DATABASE #{db_url} root root plocal;

CONNECT #{db_url} admin admin;

CREATE CLUSTER schemaless;
CREATE CLASS Schemaless CLUSTER schemaless;
""")

%{"id" => cluster_id} =
  Regex.named_captures(~r/Cluster created correctly with id #(?<id>\d+)/, output)

# Insert some records
records = [
  insert_record.("record_delete", "INSERT INTO Schemaless(name) VALUES ('record_delete');"),
  insert_record.("record_update", "INSERT INTO Schemaless(name, f) VALUES ('record_update', 'foo');"),
  insert_record.("record_load", "INSERT INTO Schemaless(name) VALUES ('record_load');")
]


defmodule TestHelpers do
  def db_name, do: unquote(db_name)

  def cluster_id, do: unquote(String.to_integer(cluster_id))

  def user,     do: System.get_env("ORIENTDB_USER")
  def password, do: System.get_env("ORIENTDB_PASS")

  def record_rid(name) do
    {_, rid} = List.keyfind(unquote(Macro.escape(records)), name, 0)
    rid
  end
end
