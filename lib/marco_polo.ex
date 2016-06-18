defmodule MarcoPolo do
  @moduledoc """
  Main API for interfacing with OrientDB.

  This module provides functions to connect to a running OrientDB server and to
  perform commands on it.

  A connection to OrientDB can be established using the `start_link/1` function
  and stoppped with `stop/1`.

  ## Connection type

  OrientDB makes a distinction between *server operations* and *database
  operations*. Server operations are operations that are performed at server
  level: examples of these operations are checking if a database exists or
  creating a new database. Database operations have to be performed inside a
  database: examples of such operations are inserting a new record or getting
  the number of records in the database.

  Server and database operations can only be performed by the appropriate
  connection: a connection to the server can perform **only** server operations,
  while a connection to a database can perform **only** database operations. The
  connection type is chosen when the connection is started via `start_link/1`.

  ## Examples

      conn_type = {:db, "GratefulDeadConcerts"}
      {:ok, conn} = MarcoPolo.start_link(user: "admin", password: "admin", connection: conn_type)
      MarcoPolo.command(conn, "SELECT FROM OUser")
      #=> {:ok, [...users...]}

  """

  alias MarcoPolo.Connection, as: C
  alias MarcoPolo.RID
  alias MarcoPolo.Document
  alias MarcoPolo.UndecodedDocument
  alias MarcoPolo.BinaryRecord
  alias MarcoPolo.Protocol
  alias MarcoPolo.Protocol.RecordSerialization

  @default_opts [
    host: "localhost",
    ssl: false,
  ]

  @default_tcp_port 2424
  @default_ssl_port 2434

  @default_fetch_plan ""

  @request_modes %{
    sync: {:raw, <<0>>},
    no_response: {:raw, <<2>>},
  }

  @tx_operation_types %{
    update: 1,
    delete: 2,
    create: 3,
  }

  @type db_type :: :document | :graph
  @type storage_type :: :plocal | :memory

  @type rec :: Document.t | BinaryRecord.t
  @type linked_records :: HashDict.t

  @type tx_operation :: {:create | :update | :delete, rec}

  @doc """
  Starts the connection with an OrientDB server.

  This function accepts the following options:

    * `:user` - (string) the OrientDB user. This option is **required**.
    * `:password` - (string) the OrientDB password. This option is **required**.
    * `:connection` - specifies the connection type. This option is
      **required**. To learn more about the connection type, refer to the docs
      for the `MarcoPolo` module (there's a "Connection type" section). It can
      be:
      * `:server` - connects to the server to perform server operations
      * `{:db, db_name}` - connects to a database to perform database
        operations.
    * `:host` - (string or charlist) the host where the OrientDB server is
      running. Defaults to `"localhost"`.
    * `:port` - (integer) the port where the OrientDB server is running.
      Defaults to `2434` for SSL connections and `2424` for normal connections.
    * `:socket_opts` - (list) options to use when opening the TCP/SSL socket.
    * `:ssl` - (boolean) whether to use SSL to connect to the OrientDB
      server. Defaults to `false`.
    * `:ssl_opts` - (list) SSL options (see the `:ssl` module for reference).

  It also accepts all options that `GenServer.start_link/3` accepts (e.g.,
  `:name` for registering the new process or `:timeout` for providing a
  connection timeout).

  ## Examples

  Connecting to the server:

      iex> {:ok, conn} = MarcoPolo.start_link user: "admin", password: "admin", connection: :server
      iex> is_pid(conn)
      true

  Connecting to a database:

      iex> connection = {:db, "MyDatabase"}
      iex> {:ok, conn} = MarcoPolo.start_link user: "admin", password: "admin", connection: connection
      iex> is_pid(conn)
      true

  """
  @spec start_link(Keyword.t) :: GenServer.on_start
  def start_link(opts \\ []) do
    Keyword.merge(@default_opts, opts)
    |> put_default_port()
    |> C.start_link()
  end

  @doc """
  Closes the connection (asynchronously), doing the required cleanup work.

  It always returns `:ok` as soon as it's called (regardless of the operation
  being successful) since it is asynchronous.

  ## Examples

      iex> MarcoPolo.stop(conn)
      :ok

  """
  @spec stop(pid) :: :ok
  def stop(conn) do
    C.stop(conn)
  end

  @doc """
  Tells if the database called `name` with the given `type` exists.

  This operation can only be performed on connections to the server. To learn
  more about the connection type, look at the "Connection type" section in the
  docs for the `MarcoPolo` module.

  ## Options

  This function accepts the following options:

    * `:timeout` - operation timeout in milliseconds. If this timeout expires,
      an exit signal will be sent to the calling process.

  ## Examples

      iex> MarcoPolo.db_exists?(conn, "GratefulDeadConcerts", :plocal)
      {:ok, true}

  """
  @spec db_exists?(pid, String.t, storage_type, Keyword.t) ::
    {:ok, boolean} | {:error, term}
  def db_exists?(conn, name, type, opts \\ []) when type in [:plocal, :memory] do
    C.operation(conn, :db_exist, [name, Atom.to_string(type)], opts)
  end

  @doc """
  Reloads the database to which `conn` is connected.

  This operation can only be performed on connections to a database. To learn
  more about the connection type, look at the "Connection type" section in the
  docs for the `MarcoPolo` module.

  ## Options

  This function accepts the following options:

    * `:timeout` - operation timeout in milliseconds. If this timeout expires,
      an exit signal will be sent to the calling process.

  ## Examples

      iex> MarcoPolo.db_reload(conn)
      :ok

  """
  @spec db_reload(pid) :: :ok | {:error, term}
  def db_reload(conn, opts \\ []) do
    case C.operation(conn, :db_reload, [], opts) do
      {:ok, _}            -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Creates a database on the server.

  `name` is used as the database name, `type` as the database type (`:document`
  or `:graph`) and `storage` as the storage type (`:plocal` or `:memory`).

  This operation can only be performed on connections to the server. To learn
  more about the connection type, look at the "Connection type" section in the
  docs for the `MarcoPolo` module.

  ## Options

  This function accepts the following options:

    * `:timeout` - operation timeout in milliseconds. If this timeout expires,
      an exit signal will be sent to the calling process.

  ## Examples

      iex> MarcoPolo.create_db(conn, "MyCoolDatabase", :document, :plocal)
      :ok

  """
  @spec create_db(pid, String.t, db_type, storage_type, Keyword.t) ::
    :ok | {:error, term}
  def create_db(conn, name, type, storage, opts \\ [])
      when type in [:document, :graph] and storage in [:plocal, :memory] do
    type    = Atom.to_string(type)
    storage = Atom.to_string(storage)

    case C.operation(conn, :db_create, [name, type, storage], opts) do
      {:ok, nil} -> :ok
      o          -> o
    end
  end

  @doc """
  Drop a database on the server.

  This function drops the database identified by the name `name` and the storage
  type `type` (either `:plocal` or `:memory`).

  This operation can only be performed on connections to the server. To learn
  more about the connection type, look at the "Connection type" section in the
  docs for the `MarcoPolo` module.

  ## Options

  This function accepts the following options:

    * `:timeout` - operation timeout in milliseconds. If this timeout expires,
      an exit signal will be sent to the calling process.

  ## Examples

      iex> MarcoPolo.drop_db(conn, "UselessDatabase", :memory)
      :ok

  """
  @spec drop_db(pid, String.t, storage_type, Keyword.t) :: :ok | {:error, term}
  def drop_db(conn, name, storage, opts \\ []) when storage in [:plocal, :memory] do
    case C.operation(conn, :db_drop, [name, Atom.to_string(storage)], opts) do
      {:ok, nil} -> :ok
      o          -> o
    end
  end

  @doc """
  Returns the size of the database to which `conn` is connected.

  This operation can only be performed on connections to a database. To learn
  more about the connection type, look at the "Connection type" section in the
  docs for the `MarcoPolo` module.

  ## Options

  This function accepts the following options:

    * `:timeout` - operation timeout in milliseconds. If this timeout expires,
      an exit signal will be sent to the calling process.

  ## Examples

      iex> MarcoPolo.db_size(conn)
      {:ok, 1158891}

  """
  @spec db_size(pid, Keyword.t) :: {:ok, non_neg_integer} | {:error, term}
  def db_size(conn, opts \\ []) do
    C.operation(conn, :db_size, [], opts)
  end

  @doc """
  Returns the number of records in the database to which `conn` is connected.

  This operation can only be performed on connections to a database. To learn
  more about the connection type, look at the "Connection type" section in the
  docs for the `MarcoPolo` module.

  ## Options

  This function accepts the following options:

    * `:timeout` - operation timeout in milliseconds. If this timeout expires,
      an exit signal will be sent to the calling process.

  ## Examples

      iex> MarcoPolo.db_countrecords(conn)
      {:ok, 7931}

  """
  @spec db_countrecords(pid, Keyword.t) :: {:ok, non_neg_integer} | {:error, term}
  def db_countrecords(conn, opts \\ []) do
    C.operation(conn, :db_countrecords, [], opts)
  end

  @doc """
  Creates a record in the database to which `conn` is connected.

  `cluster_id` specifies the cluster to create the record in, while `record` is
  the `MarcoPolo.Document` struct representing the record to create.

  The return value in case of success is `{:ok, {rid, version}}` where `rid` is
  the rid of the newly created record and `version` is the version of the newly
  created record.

  ## Options

  This function accepts the following options:

    * `:no_response` - if `true`, send the request to the OrientDB server
      without waiting for a response. This performs a *fire and forget*
      operation, returning `:ok` every time.
    * `:timeout` - operation timeout in milliseconds. If this timeout expires,
      an exit signal will be sent to the calling process.

  This operation can only be performed on connections to a database. To learn
  more about the connection type, look at the "Connection type" section in the
  docs for the `MarcoPolo` module.

  ## Examples

      iex> record = %MarcoPolo.Document{class: "MyClass", fields: %{"foo" => "bar"}}
      iex> MarcoPolo.create_record(conn, 15, record)
      {:ok, {%MarcoPolo.RID{cluster_id: 15, position: 10}, 1}}

  """
  @spec create_record(pid, non_neg_integer, rec, Keyword.t) ::
    {:ok, {RID.t, non_neg_integer}} | {:error, term}
  def create_record(conn, cluster_id, record, opts \\ []) do
    args = [{:short, cluster_id}, record, record_type(record)]

    if opts[:no_response] do
      C.no_response_operation(conn, :record_create, args ++ [@request_modes.no_response])
    else
      refetching_schema conn, fn ->
        C.operation(conn, :record_create, args ++ [@request_modes.sync], opts)
      end
    end
  end

  @doc """
  Loads a record from the database to which `conn` is connected.

  The record to load is identified by `rid`.

  ## Options

  This function accepts the following options:

    * `:fetch_plan` - the [fetching
      strategy](http://orientdb.com/docs/last/Fetching-Strategies.html) used to
      fetch the record from the database.
    * `:ignore_cache` - if `true`, the cache is ignored, if `false` it's not.
      Defaults to `true`.
    * `:load_tombstones` - if `true`, information about deleted records is
      loaded, if `false` it's not. Defaults to `false`.
    * `:if_version_not_latest` - if `true`, only load the given record if the
      version specified in the `:version` option is not the latest. If this
      option is present, the `:version` option is required. This functionality
      is supported in OrientDB >= 2.1.
    * `:version` - see the `:if_version_not_latest` option.
    * `:timeout` - operation timeout in milliseconds. If this timeout expires,
      an exit signal will be sent to the calling process.

  ## Return value

  This function returns `{:ok, resp}` in case of a successful request or
  `{:error, reason}` otherwise. In case of success, `resp` will be a two-element
  tuple with the loaded record as the first elemen, and with a set of records
  linked to it as the second element. This set of linked records can be
  controlled via the `:fetch_plan` options. You're not supposed to manipulate
  this value directly (so that the implementation can stay flexible); use the
  functions in the `MarcoPolo.FetchPlan` module to work with linked records.

  This operation can only be performed on connections to a database. To learn
  more about the connection type, look at the "Connection type" section in the
  docs for the `MarcoPolo` module.

  ## Examples

      iex> rid = %MarcoPolo.RID{cluster_id: 10, position: 184}
      iex> {:ok, {record, linked_records}} = MarcoPolo.load_record(conn, rid)
      iex> record.fields
      %{"foo" => "bar"}

  """
  @spec load_record(pid, RID.t, Keyword.t) :: {:ok, {rec, linked_records}} | {:error, term}
  def load_record(conn, %RID{} = rid, opts \\ []) do
    {op, args} =
      if opts[:if_version_not_latest] do
        args = [{:short, rid.cluster_id},
                {:long, rid.position},
                {:int, Keyword.fetch!(opts, :version)},
                opts[:fetch_plan] || @default_fetch_plan,
                opts[:ignore_cache] || true,
                opts[:load_tombstones] || false]
        {:record_load_if_version_not_latest, args}
      else
        args = [{:short, rid.cluster_id},
                {:long, rid.position},
                opts[:fetch_plan] || @default_fetch_plan,
                opts[:ignore_cache] || true,
                opts[:load_tombstones] || false]
        {:record_load, args}
      end

    refetching_schema conn, fn ->
      C.operation(conn, op, args, opts)
    end
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

  When the update is successful, `{:ok, new_version}` is returned; otherwise,
  `{:error, reason}`.

  ## Options

  This function accepts the following options:

    * `:no_response` - if `true`, send the request to the OrientDB server
      without waiting for a response. This performs a *fire and forget*
      operation, returning `:ok` every time.
    * `:timeout` - operation timeout in milliseconds. If this timeout expires,
      an exit signal will be sent to the calling process.

  This operation can only be performed on connections to a database. To learn
  more about the connection type, look at the "Connection type" section in the
  docs for the `MarcoPolo` module.

  ## Examples

      iex> rid = %MarcoPolo.RID{cluster_id: 1, position: 10}
      iex> new_record = %MarcoPolo.Document{class: "MyClass", fields: %{foo: "new value"}}
      iex> MarcoPolo.update_record(conn, rid, 1, new_record, true)
      {:ok, 2}

  """
  @spec update_record(pid, RID.t, non_neg_integer, Document.t, boolean, Keyword.t) ::
    {:ok, non_neg_integer} | {:error, term}
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
      C.operation(conn, :record_update, args ++ [@request_modes.sync], opts)
    end
  end

  @doc """
  Deletes a record from the database to which `conn` is connected.

  The record to delete is identified by `rid`; version `version` is
  deleted. Returns `{:ok, deleted?}` where `deleted?` is a boolean that tells if
  the record has been deleted.

  ## Options

  This function accepts the following options:

    * `:no_response` - if `true`, send the request to the OrientDB server
      without waiting for a response. This performs a *fire and forget*
      operation, returning `:ok` every time.
    * `:timeout` - operation timeout in milliseconds. If this timeout expires,
      an exit signal will be sent to the calling process.

  This operation can only be performed on connections to a database. To learn
  more about the connection type, look at the "Connection type" section in the
  docs for the `MarcoPolo` module.

  ## Examples

      iex> rid = %MarcoPolo.RID{cluster_id: 76, position: 12}
      iex> MarcoPolo.delete_record(conn, rid, 1)
      {:ok, true}

  """
  @spec delete_record(pid, RID.t, non_neg_integer, Keyword.t) ::
    {:ok, boolean} | {:error, term}
  def delete_record(conn, %RID{} = rid, version, opts \\ []) do
    args = [{:short, rid.cluster_id},
            {:long, rid.position},
            {:int, version}]

    if opts[:no_response] do
      C.no_response_operation(conn, :record_delete, args ++ [@request_modes.no_response])
    else
      C.operation(conn, :record_delete, args ++ [@request_modes.sync], opts)
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

  ## Options

  The options that this function accepts depend in part on the type of the operation.

  The options shared by both idempotent and non-idempotent operations are the following:

    * `:params` - a map of params with atoms or strings as keys and any
      encodable term as values. These parameters are used by OrientDB to build
      prepared statements as you can see in the examples below. Defaults to `%{}`.
    * `:timeout` - operation timeout in milliseconds. If this timeout expires,
      an exit signal will be sent to the calling process.

  The additional options for idempotent (e.g., `SELECT`) queries are:

    * `:fetch_plan`: a string specifying the fetch plan. Mandatory for `SELECT`
      queries.

  ## Return value

  If the query is successful then the return value is an `{:ok, values}`
  tuple. `values` is a map with the following keys:

    * `:response` - depends on the performed query. For example a `SELECT` query
      will return a list of records, while a `CREATE CLUSTER` command will
      return a cluster id.
    * `:linked_records` - it's a set of additional records that have been
      fetched by OrientDB. This can be controlled using a [fetch
      plan](https://orientdb.com/docs/last/Fetching-Strategies.html) in the
      query. You're not supposed to manipulate this value directly (so that the
      implementation can stay flexible); use the functions in the
      `MarcoPolo.FetchPlan` module to work with linked records.

  This operation can only be performed on connections to a database. To learn
  more about the connection type, look at the "Connection type" section in the
  docs for the `MarcoPolo` module.

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
  @spec command(pid, String.t, Keyword.t)
    :: {:ok, %{response: term, linked_records: linked_records}} | {:error, term}
  def command(conn, query, opts \\ []) do
    query_type = query_type(query)

    command_class_name =
      case query_type do
        :sql_query   -> "q"
        :sql_command -> "c"
      end

    command_class_name = Protocol.Types.encode(command_class_name)

    payload = encode_query_with_type(query_type, query, opts)

    args = [{:raw, "s"}, # synchronous mode
            IO.iodata_length([command_class_name, payload]),
            {:raw, command_class_name},
            {:raw, payload}]

    refetching_schema conn, fn ->
      C.operation(conn, :command, args, opts)
    end
  end

  @doc """
  Executes a script in the given `language` on the database `conn` is connected
  to.

  The text of the script is passed as `text`. `opts` is a list of options.

  **Note**: for this to work, scripting must be enabled in the server
  configuration. You can read more about scripting in the [OrientDB
  docs](http://orientdb.com/docs/last/Javascript-Command.html#Enable_Server_side_scripting).

  This operation can only be performed on connections to a database. To learn
  more about the connection type, look at the "Connection type" section in the
  docs for the `MarcoPolo` module.

  ## Options

  This function accepts the following options:

    * `:timeout` - operation timeout in milliseconds. If this timeout expires,
      an exit signal will be sent to the calling process.

  ## Examples

      iex> script = "for (i = 0; i < 3; i++) db.command('INSERT INTO Foo(idx) VALUES (' + i + ')');"
      iex> {:ok, last_record} = MarcoPolo.script(conn, "Javascript", script)
      iex> last_record.fields["idx"]
      2

  """
  @spec script(pid, String.t, String.t, Keyword.t) :: {:ok, term} | {:error, term}
  def script(conn, language, text, opts \\ []) do
    command_class_name = Protocol.Types.encode("s")

    payload = [Protocol.Types.encode(language),
               encode_query_with_type(:sql_command, text, opts)]

    args = [{:raw, "s"}, # synchronous mode
            IO.iodata_length([command_class_name, payload]),
            {:raw, command_class_name},
            {:raw, payload}]

    refetching_schema conn, fn ->
      C.operation(conn, :command, args, opts)
    end
  end

  @doc """
  Runs operations inside a transaction on the database to which `conn` is
  connected.

  This function will run a list of `operations` inside a server-side
  transaction. Operations can be creations, updates and deletions of
  records. Each operation has the form:

      {op_type, record}

  where `op_type` can be one of `:create`, `:update` or `:delete`.

  ## Options

  This function accepts the following options:

    * `:using_tx_log` - tells the server whether to use the transaction log to
      recover the transaction or not. Defaults to `true`. *Note*: disabling the
      log could speed up the execution of the transaction, but it makes
      impossible to rollback the transaction in case of errors. This could lead
      to inconsistencies in indexes as well, since in case of duplicated keys
      the rollback is not called to restore the index status.
    * `:timeout` - operation timeout in milliseconds. If this timeout expires,
      an exit signal will be sent to the calling process.

  ## Examples

      iex> ops = [{:create, %MarcoPolo.Document{class: "Foo", fields: %{"foo" => "bar"}}},
      ...>        {:delete, %MarcoPolo.Document{rid: %MarcoPolo.RID{cluster_id: 1, position: 2}}]
      iex> {:ok, %{created: [created], updated: []} = MarcoPolo.transaction(conn, ops)
      iex> created
      %MarcoPolo.Document{class: "Foo", fields: %{"foo" => "bar"}, rid: %MarcoPolo.RID{...}}

  """
  @spec transaction(pid, [tx_operation], Keyword.t) ::
    {:ok, %{created: [{RID.t, non_neg_integer}], updated: [{RID.t, non_neg_integer}]}} |
    {:error, term}
  def transaction(conn, operations, opts \\ []) when is_list(operations) do
    {op_args, _index} = Enum.flat_map_reduce operations, -2, fn
      {:create, _} = op, i ->
        {args_from_tx_operation(op, i, opts), i - 1}
      op, acc ->
        {args_from_tx_operation(op, nil, opts), acc}
    end

    # The 0 at the end signals the end of the record list, while the empty
    # binary is there because OrientDB :). It has to do with index changes, and
    # it's not in the docs because OrientDB :).
    args = [:transaction_id, opts[:using_tx_log] || true]
      ++ op_args
      ++ [{:raw, <<0>>}, <<>>]

    C.operation(conn, :tx_commit, args, opts)
  end

  @doc """
  Subscribes to a Live Query for the given `query`.

  This function subscribes to a [Live
  Query](https://orientdb.com/docs/last/Live-Query.html) for the given
  `query`. Every time a change happens in the given query, a message will be
  sent to `receiver`.

  If the subscription is successful, this function returns `{:ok, token}` where
  `token` is a unique identifier for the subscription to the given live
  query. It's important to keep it around as it's needed to unsubscribe from the
  live query (see `live_query_unsubscribe/2`).

  The messages sent to `receiver` each time there's a change in the live query
  have the following structure:

      {:orientdb_live_query, token, message}
      {:orientdb_live_query, token, {operation, record}}

  where:

    * `token` is the token mentioned above
    * `message` can be:
      * `:unsubscribed` if `live_query_unsubscribe` was successful
      * `{operation, record}`, where `operation` is one of `:create`, `:update`,
        or `:delete`, based on the operation happened on the server, and
        `record` is the subject of the operation happened on the server

  ## Options

  This function accepts the following options:

    * `:timeout` - operation timeout in milliseconds. If this timeout expires,
      an exit signal will be sent to the calling process.

  ## Examples

      {:ok, token} = MarcoPolo.live_query(conn, "LIVE SELECT FROM Language", self())

  Now, another client performs this query:

      INSERT INTO Language(name, creators) VALUES ('Elixir', 'José Valim')

  Back to the process that started the live query:

      receive do
        {:orientdb_live_query, ^token, {operation, record}} ->
          operation #=> :create
          record.fields["name"] #=> "Elixir"
      end

  """
  @spec live_query(pid, String.t, pid, Keyword.t) :: {:ok, integer} | {:error, term}
  def live_query(conn, query, receiver, opts \\ []) do
    command_class_name = Protocol.Types.encode("q") # always a query, never a command

    payload = Protocol.Types.encode_list [
      query,
      -1,
      opts[:fetch_plan] || @default_fetch_plan,
      %Document{fields: %{"params" => %{}}},
    ]

    args = [
      {:raw, "l"}, # live query mode
      IO.iodata_length([command_class_name, payload]),
      {:raw, command_class_name},
      {:raw, payload},
    ]

    C.live_query(conn, args, receiver, opts)
  end

  @doc """
  Unsubscribes from the live query identified by `token`.

  This function unsubscribes from the live query identified by `token` (see
  `live_query/4`). Once an unsubscription from a live query happens, no more
  messages will be sent to the receiver specified when the live query had been
  started.

  This operation happens asynchronously: when the unsubscription happens, the
  receiver will receive a `{:orientdb_live_query, token, :unsubscribed}`
  message.

  ## Examples

      iex> MarcoPolo.live_query_unsubscribe(conn, token)
      :ok

  """
  @spec live_query_unsubscribe(pid, integer) :: :ok
  def live_query_unsubscribe(conn, token) when is_integer(token) do
    string_token = Integer.to_string(token)

    case command(conn, "LIVE UNSUBSCRIBE #{string_token}") do
      {:ok, %{response: %Document{fields: %{"unsubscribed" => ^string_token}}}} ->
        :ok
      o ->
        o
    end
  end

  @doc """
  Fetches the distributed configuration of the OrientDB server.

  The OrientDB server will push data to clients (including MarcoPolo) when the
  distributed configuration of the server changes. This configuration (a
  `MarcoPolo.Document`) is stored by MarcoPolo and can be retrieved through this
  function.

  ## Options

  This function accepts the following options:

    * `:timeout` - operation timeout in milliseconds. If this timeout expires,
      an exit signal will be sent to the calling process.

  """
  @spec distrib_config(pid) :: Document.t
  def distrib_config(conn, opts \\ []) do
    C.distrib_config(conn, opts)
  end

  defp encode_query_with_type(:sql_query, query, opts) do
    args = [query,
            -1,
            opts[:fetch_plan] || @default_fetch_plan,
            %Document{class: nil, fields: %{"params" => to_params(opts[:params] || %{})}}]

    Protocol.Types.encode_list(args)
  end

  defp encode_query_with_type(:sql_command, query, opts) do
    args = [query]

    args=
      if params = opts[:params] do
        params = %Document{class: nil, fields: %{"parameters" => to_params(params)}}
        # `true` means "use simple parameters".
        args ++ [true, params]
      else
        args ++ [false]
      end

    args = args ++ [false]

    Protocol.Types.encode_list(args)
  end

  defp refetching_schema(conn, fun) do
    case fun.() do
      {:ok, %{response: response, linked_records: linked} = r} ->
        if unknown_property_ids?(response) or unknown_property_ids?(linked) do
          schema = C.fetch_schema(conn)
          response = redecode_with_new_schema(response, schema)
          linked = redecode_with_new_schema(linked, schema)
          {:ok, %{r | response: response, linked_records: linked}}
        else
          {:ok, r}
        end
      {:ok, {record, linked} = r} ->
        if unknown_property_ids?(record) or unknown_property_ids?(linked) do
          schema = C.fetch_schema(conn)
          record = redecode_with_new_schema(record, schema)
          linked = redecode_with_new_schema(linked, schema)
          {:ok, {record, linked}}
        else
          {:ok, r}
        end
      o ->
        o
    end
  end

  defp unknown_property_ids?(%UndecodedDocument{}),
    do: true
  defp unknown_property_ids?(records) when is_list(records),
    do: Enum.any?(records, &unknown_property_ids?/1)
  defp unknown_property_ids?(%HashDict{} = records),
    do: Enum.any?(records, &unknown_property_ids?/1)
  defp unknown_property_ids?(_),
    do: false

  defp redecode_with_new_schema(records, schema) when is_list(records) do
    Enum.map(records, &redecode_with_new_schema(&1, schema))
  end

  defp redecode_with_new_schema(%UndecodedDocument{rid: rid, version: vsn, content: content}, schema) do
    doc = RecordSerialization.decode(content, schema)
    %{doc | version: vsn, rid: rid}
  end

  defp redecode_with_new_schema(%HashDict{} = records, schema) do
    for {rid, record} <- records, into: HashDict.new do
      {rid, redecode_with_new_schema(record, schema)}
    end
  end

  defp redecode_with_new_schema(record, _schema) do
    record
  end

  defp to_params(params) when is_map(params) do
    params
  end

  defp to_params(params) when is_list(params) do
    params
    |> Stream.with_index
    |> Stream.map(fn({val, i}) -> {i, val} end)
    |> Enum.into(%{})
  end

  defp query_type(query) do
    case query_command(query) do
      cmd when cmd in ["select", "traverse"] ->
        :sql_query
      _ ->
        :sql_command
    end
  end

  defp query_command(query) do
    regex               = ~r/^\s*(?<cmd>\w+)/
    %{"cmd" => command} = Regex.named_captures(regex, query)

    String.downcase(command)
  end

  defp args_from_tx_operation({:create, record}, incr_position, _opts) do
    # -1 is the cluster id for the record that doesn't exist yet. OrientDB wants
    # -it this way.
    [
      {:raw, <<1>>}, # continue to read records
      {:raw, <<@tx_operation_types.create>>},
      {:short, -1},
      {:long, incr_position},
      record_type(record),
      record,
    ]
  end

  defp args_from_tx_operation({:update, %{__struct__: _, rid: %RID{} = rid} = record}, _incr_position, opts) do
    unless record.version do
      raise MarcoPolo.Error, "missing :version in the record #{inspect record}"
    end

    [
      {:raw, <<1>>}, # continue to read records
      {:raw, <<@tx_operation_types.update>>},
      {:short, rid.cluster_id},
      {:long, rid.position},
      record_type(record),
      {:int, record.version},
      record,
      opts[:update_content] || true,
    ]
  end

  defp args_from_tx_operation({:delete, %{__struct__: _, rid: %RID{} = rid} = record}, _incr_position, _opts) do
    unless record.version do
      raise MarcoPolo.Error, "missing :version in the record #{inspect record}"
    end

    [
      {:raw, <<1>>}, # continue to read records
      {:raw, <<@tx_operation_types.delete>>},
      {:short, rid.cluster_id},
      {:long, rid.position},
      record_type(record),
      {:int, record.version},
    ]
  end

  defp record_type(%Document{}), do: {:raw, "d"}
  defp record_type(%BinaryRecord{}), do: {:raw, "b"}

  defp put_default_port(opts) do
    port = if opts[:ssl], do: @default_ssl_port, else: @default_tcp_port
    Keyword.put(opts, :port, port)
  end
end
