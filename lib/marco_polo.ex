defmodule MarcoPolo do
  alias MarcoPolo.Connection, as: C
  alias MarcoPolo.RID
  alias MarcoPolo.Document
  alias MarcoPolo.Protocol

  @default_opts [
    host: "localhost",
    port: 2424,
  ]

  @request_modes %{
    sync: {:raw, <<0>>},
    no_response: {:raw, <<2>>},
  }

  @doc """
  Starts the connection with an OrientDB server.

  This function accepts the following options:

    * `:user` - (string) the OrientDB user. This option is **required**.
    * `:pass` - (string) the OrientDB password. This option is **required**.
    * `:connection` - specifies the connection type. To connect to the OrientDB
      server (to perform server operations) this option must have the value
      `:server`; to connect to a database, this option must have the value
      `{:db, db_name, db_type}`.
    * `:host` - (string) the host where the OrientDB server is running. Defaults
      to `"localhost"`.
    * `:port` - (integer) the port where the OrientDB server is
      running. Defaults to `2424`.

  """
  @spec start_link(Keyword.t) :: GenServer.on_start
  def start_link(opts \\ []) do
    opts = Keyword.merge(@default_opts, opts)

    unless Keyword.get(opts, :connection) do
      raise ArgumentError, "no connection type (connect/db_open) specified"
    end

    C.start_link(opts)
  end

  @doc """
  Tells whether the database called `name` with the given `type` exists.

  ## Examples

      iex> MarcoPolo.db_exists?(conn, "GratefulDeadConcerts", "plocal")
      {:ok, true}

  """
  @spec db_exists?(pid, String.t, String.t) :: {:ok, boolean}
  def db_exists?(conn, name, type) do
    C.operation(conn, :db_exist, [name, type])
  end

  @doc """
  Reloads the database to which `conn` is connected.

  ## Examples

      iex> MarcoPolo.db_reload(conn)
      :ok

  """
  @spec db_reload(pid) :: :ok
  def db_reload(conn) do
    case C.operation(conn, :db_reload, []) do
      {:ok, _}            -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Creates a database on the server.

  `name` is used as the database name, `type` as the database type (`:document`
  or `:graph`) and `storage` as the storage type (`:plocal` or `:memory`).

  ## Examples

      iex> MarcoPolo.create_db(conn, "MyCoolDatabase", :document, :plocal)
      :ok

  """
  @spec create_db(pid, String.t, :document | :graph, :plocal | :memory) :: :ok
  def create_db(conn, name, type, storage)
      when type in [:document, :graph] and storage in [:plocal, :memory] do
    type    = Atom.to_string(type)
    storage = Atom.to_string(storage)

    case C.operation(conn, :db_create, [name, type, storage]) do
      {:ok, nil} -> :ok
      o          -> o
    end
  end

  @doc """
  Drop a database on the server.

  This function drops the database identified by the name `name` and the storage
  type `type` (either `:plocal` or `:memory`).

  ## Examples

      iex> MarcoPolo.drop_db(conn, "UselessDatabase", :memory)
      :ok

  """
  @spec drop_db(pid, String.t, :plocal | :memory) :: :ok
  def drop_db(conn, name, storage) when storage in [:plocal, :memory] do
    case C.operation(conn, :db_drop, [name, Atom.to_string(storage)]) do
      {:ok, nil} -> :ok
      o          -> o
    end
  end

  @doc """
  Returns the size of the database to which `conn` is connected.

  ## Examples

      iex> MarcoPolo.db_size(conn)
      {:ok, 1158891}

  """
  @spec db_size(pid) :: {:ok, non_neg_integer}
  def db_size(conn) do
    C.operation(conn, :db_size, [])
  end

  @doc """
  Returns the number of records in the database to which `conn` is connected.

  ## Examples

      iex> MarcoPolo.db_countrecords(conn)
      {:ok, 7931}

  """
  @spec db_countrecords(pid) :: {:ok, non_neg_integer}
  def db_countrecords(conn) do
    C.operation(conn, :db_countrecords, [])
  end

  @doc """
  Creates a record in the database to which `conn` is connected.

  `cluster_id` specifies the cluster to create the record in, while `record` is
  the `MarcoPolo.Document` struct representing the record to create.

  The return value in case of success is `{:ok, {rid, version}}` where `rid` is
  the rid of the newly created record and `version` is the version of the newly
  created record.

  This function accepts the following options:

    * `:no_response` - if `true`, send the request to the OrientDB server
      without waiting for a response. This performs a *fire and forget*
      operation, returning `:ok` every time.

  ## Examples

      iex> record = %MarcoPolo.Document{class: "MyClass", fields: %{"foo" => "bar"}}
      iex> MarcoPolo.create_record(conn, 15, record)
      {:ok, {%MarcoPolo.RID{cluster_id: 15, position: 10}, 1}}

  """
  @spec create_record(pid, non_neg_integer, Document.t, Keyword.t) ::
    {:ok, {RID.t, non_neg_integer}}
  def create_record(conn, cluster_id, record, opts \\ []) do
    args = [{:short, cluster_id}, record, {:raw, "d"}]

    if opts[:no_response] do
      C.no_response_operation(conn, :record_create, args ++ [@request_modes.no_response])
    else
      C.operation(conn, :record_create, args ++ [@request_modes.sync])
    end
  end

  @doc """
  Loads a record from the database to which `conn` is connected.

  The record to load is identified by `rid`. `fetch_plan` is the [fetching
  strategy](http://orientdb.com/docs/last/Fetching-Strategies.html) used to
  fetch the record from the database. Since multiple records could be returned,
  the return value is `{:ok, list_of_records}`.

  This function accepts a list of options (`opts`):

    * `:ignore_cache` - if `true`, the cache is ignored, if `false` it's not.
      Defaults to `true`.
    * `:load_tombstones` - if `true`, information about deleted records is
      loaded, if `false` it's not. Defaults to `false`.
    * `:if_version_not_latest` - if `true`, only load the given record if the
      version specified in the `:version` option is not the latest. If this
      option is present, the `:version` option is required. This functionality
      is supported in OrientDB >= 2.1.

  ## Examples

      iex> rid = %MarcoPolo.RID{cluster_id: 10, position: 184}
      iex> {:ok, [record]} = MarcoPolo.load_record(conn, rid, "*:-1")
      iex> record.fields
      %{"foo" => "bar"}

  """
  @spec load_record(pid, RID.t, String.t, Keyword.t) :: {:ok, [Document.t]}
  def load_record(conn, %RID{} = rid, fetch_plan, opts \\ []) do
    {op, args} =
      if opts[:if_version_not_latest] do
        args = [{:short, rid.cluster_id},
                {:long, rid.position},
                {:int, Keyword.fetch!(opts, :version)},
                fetch_plan,
                opts[:ignore_cache] || true,
                opts[:load_tombstones] || false]
        {:record_load_if_version_not_latest, args}
      else
        args = [{:short, rid.cluster_id},
                {:long, rid.position},
                fetch_plan,
                opts[:ignore_cache] || true,
                opts[:load_tombstones] || false]
        {:record_load, args}
      end


    C.operation(conn, op, args)
  end

  @doc """
  Updates the given record in the databse to which `conn` is connected.

  The record to update is identified by its `rid`; `version` is the version to
  update. `new_record` is the updated record. `update_content?` can be:

    * `true` - the content of the record has been changed and should be updated
      in the storage.
    * `false` - the record was modified but its own content has not changed:
      related collections (e.g. RidBags) have to be updated, but the record
      version and its contents should not be updated.

  When the update is successful, `{:ok, new_version}` is returned.

  This function accepts the following options:

    * `:no_response` - if `true`, send the request to the OrientDB server
      without waiting for a response. This performs a *fire and forget*
      operation, returning `:ok` every time.

  ## Examples

      iex> rid = %MarcoPolo.RID{cluster_id: 1, position: 10}
      iex> new_record = %MarcoPolo.Document{class: "MyClass", fields: %{foo: "new value"}}
      iex> MarcoPolo.update_record(conn, rid, 1, new_record, true)
      {:ok, 2}

  """
  @spec update_record(pid, RID.t, non_neg_integer, Document.t, boolean, Keyword.t) ::
    {:ok, non_neg_integer}
  def update_record(conn, %RID{} = rid, version, new_record, update_content?, opts \\ []) do
    args = [{:short, rid.cluster_id},
            {:long, rid.position},
            update_content?,
            new_record,
            version,
            {:raw, "d"}]

    if opts[:no_response] do
      C.no_response_operation(conn, :record_update, args ++ [@request_modes.no_response])
    else
      C.operation(conn, :record_update, args ++ [@request_modes.sync])
    end
  end

  @doc """
  Deletes a record from the database to which `conn` is connected.

  The record to delete is identified by `rid`; version `version` is
  deleted. Returns `{:ok, deleted?}` where `deleted?` is a boolean that tells
  whether the record has been deleted.

  ## Examples

      iex> rid = %MarcoPolo.RID{cluster_id: 76, position: 12}
      iex> MarcoPolo.delete_record(conn, rid, 1)
      {:ok, true}

  """
  @spec delete_record(pid, RID.t, non_neg_integer, Keyword.t) :: {:ok, boolean}
  def delete_record(conn, %RID{} = rid, version, opts \\ []) do
    args = [{:short, rid.cluster_id},
            {:long, rid.position},
            {:int, version}]

    if opts[:no_response] do
      C.no_response_operation(conn, :record_delete, args ++ [@request_modes.no_response])
    else
      C.operation(conn, :record_delete, args ++ [@request_modes.sync])
    end
  end

  @doc """
  Execute the given `query` in the database to which `conn` is connected.

  OrientDB makes a distinction between idempotent queries and non-idempotent
  queries (it calls the former *queries* and the latter *commands*). In order to
  provide a clean interface for performing operations on the server, `MarcoPolo`
  provides only a `command/3` function both for idempotent as well as
  non-idempotent operations. Whether an operation is idempotent is inferred by
  the text in `query`. As of now, `SELECT` and `TRAVERSE` operations are
  idempotent while all other operations are non-idempotent.

  The options that this function accepts depend in part on the type of the operation.

  The options shared by both idempotent and non-idempotent operations are the following:

    * `:params` - a map of params with atoms or strings as keys and any
      encodable term as values. These parameters are used by OrientDB to build
      prepared statements as you can see in the examples below. Defaults to `%{}`.

  The additional options for idempotent (e.g., `SELECT`) queries are:

    * `:fetch_plan`: a string specifying the fetch plan. Mandatory for `SELECT`
      queries.

  If the query is successful then the return value is an `{:ok, values}` tuple
  where `values` strictly depends on the performed query. Usually, `values` is a
  list of results. For example, when a `CREATE CLUSTER` command is executed,
  `{:ok, [cluster_id]}` is returned where `cluster_id` is the id of the newly
  created cluster.
  query.

  ## Examples

  The following is an example of an idempotent command:

      iex> opts = [params: %{name: "jennifer"}, fetch_plan: "*:-1"]
      iex> query = "SELECT FROM User WHERE name = :name AND age > 18"
      iex> {:ok, %MarcoPolo.Document{} = doc} = MarcoPolo.command(conn, query, opts)
      iex> doc.fields["name"]
      "jennifer"
      iex> doc.fields["age"]
      45

  The following is an example of a non-idempotent command:

      iex> query = "INSERT INTO User(name) VALUES ('meg', 'abed')"
      iex> {:ok, [meg, abed]} = MarcoPolo.command(conn, query)
      iex> meg.fields["name"]
      "meg"
      iex> abed.fields["name"]
      "abed"

  """
  @spec command(pid, String.t, Keyword.t) :: term
  def command(conn, query, opts \\ []) do
    query_type = MarcoPolo.QueryParser.query_type(query)

    command_class_name =
      case query_type do
        :sql_query   -> "q"
        :sql_command -> "c"
      end

    command_class_name = Protocol.encode_term(command_class_name)

    payload = encode_query_with_type(query_type, query, opts)

    args = [{:raw, "s"}, # synchronous mode
            IO.iodata_length([command_class_name, payload]),
            {:raw, command_class_name},
            {:raw, payload}]

    C.operation(conn, :command, args)
  end

  @doc """
  Executes a script in the given `language` on the database `conn` is connected
  to.

  The text of the script is passed as `text`. `opts` is a list of options.

  **Note**: for this to work, scripting must be enabled in the server
  configuration. You can read more about scripting in the [OrientDB
  docs](http://orientdb.com/docs/last/Javascript-Command.html#Enable_Server_side_scripting).

  ## Examples

      iex> script = "for (i = 0; i < 3; i++) db.command('INSERT INTO Foo(idx) VALUES (' + i + ')');"
      iex> {:ok, last_record} = MarcoPolo.script(conn, "Javascript", script)
      iex> last_record.fields["idx"]
      2

  """
  @spec script(pid, String.t, String.t, Keyword.t) :: {:ok, term}
  def script(conn, language, text, opts \\ []) do
    command_class_name = Protocol.encode_term("s")

    payload = [Protocol.encode_term(language),
               encode_query_with_type(:sql_command, text, opts)]

    args = [{:raw, "s"}, # synchronous mode
            IO.iodata_length([command_class_name, payload]),
            {:raw, command_class_name},
            {:raw, payload}]

    C.operation(conn, :command, args)
  end

  @doc """
  Tells the connection (`conn`) to re-fetch the schema of the OrientDB database.

  This is usually used when an `:unknown_property_id` error is returned in orded
  to fetch the schema again and make all the new properties available.

  This function operates as a "cast" (*fire and forget*) operation, so it
  returns `:ok` right away.

  ## Examples

      iex> MarcoPolo.fetch_schema(conn)
      :ok

  """
  @spec fetch_schema(pid) :: :ok
  def fetch_schema(conn) do
    C.fetch_schema(conn)
  end

  defp encode_query_with_type(:sql_query, query, opts) do
    params = opts[:params] || %{}
    args = [query,
            -1,
            Keyword.fetch!(opts, :fetch_plan),
            %Document{class: nil, fields: %{"params" => params}}]

    Protocol.encode_list_of_terms(args)
  end

  defp encode_query_with_type(:sql_command, query, opts) do
    args = [query]

    if params = opts[:params] do
      params = %Document{class: nil, fields: %{"parameters" => params}}
      # `true` means "use simple parameters".
      args = args ++ [true, params]
    else
      args = args ++ [false]
    end

    args = args ++ [false]

    Protocol.encode_list_of_terms(args)
  end
end
