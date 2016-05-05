# Ignore scripting tests by default as scripting must be enabled manually in the
# OrientDB server configuration. Same goes for Live Query.
excludes = [:scripting, :live_query, :ssl]
ExUnit.configure(exclude: (ExUnit.configuration[:exclude] || []) ++ excludes)

ExUnit.start()

unless :integration in ExUnit.configuration[:exclude] do
  user = System.get_env("ORIENTDB_USER") ||
    Mix.raise """
    The $ORIENTDB_USER variable is empty, but it needs to be set to an
    OrientDB admin username in order to run MarcoPolo tests.
    """

  pass = System.get_env("ORIENTDB_PASS") ||
    Mix.raise """
    The $ORIENTDB_PASS variable is empty, but it needs to be set to the
    password for the user specified in the $ORIENTDB_USER variable in order to
    run MarcoPolo tests.
    """

  case :gen_tcp.connect('localhost', 2424, []) do
    {:ok, _} ->
      :ok
    {:error, reason} ->
      Mix.raise "Error connecting to OrientDB in test_helper.exs: #{:inet.format_error(reason)}"
  end

  clusters = []
  records  = []

  run_script = fn(script) ->
    case System.cmd("orientdb-console", [script], stderr_to_stdout: true) do
      {lines, 0}   -> lines
      {err, _status} -> Mix.raise """
      Database setup in test/test_helper.exs failed:
      #{err}
      """
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
    CONNECT remote:localhost/MarcoPoloTest #{user} #{pass};
    #{insert_cmd}
    """)

    {name, extract_rid.(output)}
  end

  run_script.("""
  SET ignoreErrors true;
  DROP DATABASE remote:localhost/MarcoPoloTest #{user} #{pass};
  DROP DATABASE remote:localhost/MarcoPoloTestGenerated #{user} #{pass};
  DROP DATABASE remote:localhost/MarcoPoloToDrop #{user} #{pass};
  SET ignoreErrors false;

  CREATE DATABASE remote:localhost/MarcoPoloTest #{user} #{pass} plocal;
  CREATE DATABASE remote:localhost/MarcoPoloToDrop #{user} #{pass} memory;
  """)

  output = run_script.("""
  CONNECT remote:localhost/MarcoPoloTest #{user} #{pass};
  CREATE CLUSTER schemaless ID 999;
  CREATE CLASS Schemaless CLUSTER 999;
  """)

  clusters = [{"schemaless", extract_cluster_id.(output)}|clusters]

  output = run_script.("""
  CONNECT remote:localhost/MarcoPoloTest #{user} #{pass};
  CREATE CLUSTER schemaful ID 1001;
  CREATE CLASS Schemaful;
  CREATE PROPERTY Schemaful.myString STRING;
  """)

  clusters = [{"schemaful", extract_cluster_id.(output)}|clusters]

  # Insert some records
  records = records ++ [
    insert_record.("record_delete", "INSERT INTO Schemaless(name) VALUES ('record_delete');"),
    insert_record.("record_update", "INSERT INTO Schemaless(name, f) VALUES ('record_update', 'foo');"),
    insert_record.("record_load", "INSERT INTO Schemaless(name, f) VALUES ('record_load', 'foo');"),
    insert_record.("schemaless_record_load", "INSERT INTO Schemaful(myString) VALUES ('record_load');"),
  ]

  cacertfile =
    if :ssl in ExUnit.configuration[:include] do
      System.get_env("ORIENTDB_CACERTFILE") ||
        Mix.raise """
        The $ORIENTDB_CACERTFILE variable is not set. It should point to the
        PEM certificate for the OrientDB server.
        """
    else
      nil
    end

  defmodule TestHelpers do
    def user,       do: unquote(user)
    def password,   do: unquote(pass)
    def cacertfile, do: unquote(cacertfile)

    def cluster_id(name) do
      {_, id} = List.keyfind(unquote(Macro.escape(clusters)), name, 0)
      id
    end

    def record_rid(name) do
      {_, rid} = List.keyfind(unquote(Macro.escape(records)), name, 0)
      rid
    end
  end
end
