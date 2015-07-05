defmodule MarcoPolo do
  alias MarcoPolo.Connection, as: C
  alias MarcoPolo.RID
  alias MarcoPolo.Record
  alias MarcoPolo.Protocol

  @default_opts [
    host: "localhost",
    port: 2424,
  ]

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
  the `MarcoPolo.Record` struct representing the record to create.

  The return value in case of success is `{:ok, {rid, version}}` where `rid` is
  the rid of the newly created record and `version` is the version of the newly
  created record.

  ## Examples

      iex> record = %MarcoPolo.Record{class: "MyClass", fields: %{"foo" => "bar"}}
      iex> MarcoPolo.create_record(conn, 15, record)
      {:ok, {%MarcoPolo.RID{cluster_id: 15, position: 10}, 1}}

  """
  @spec create_record(pid, non_neg_integer, Record.t) ::
    {:ok, {RID.t, non_neg_integer}}
  def create_record(conn, cluster_id, record) do
    args = [{:short, cluster_id}, record, {:raw, "d"}, {:raw, <<0>>}]

    case C.operation(conn, :record_create, args) do
      {:ok, [cluster_id, position, version]} ->
        rid = %RID{cluster_id: cluster_id, position: position}
        {:ok, {rid, version}}
      o ->
        o
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
  @spec load_record(pid, RID.t, String.t, Keyword.t) :: {:ok, [Record.t]}
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
  Deletes a record from the database to which `conn` is connected.

  The record to delete is identified by `rid`; version `version` is
  deleted. Returns `{:ok, deleted?}` where `deleted?` is a boolean that tells
  whether the record has been deleted.

  ## Examples

      iex> rid = %MarcoPolo.RID{cluster_id: 76, position: 12}
      iex> MarcoPolo.delete_record(conn, rid, 1)
      {:ok, true}

  """
  @spec delete_record(pid, RID.t, non_neg_integer) :: {:ok, boolean}
  def delete_record(conn, %RID{} = rid, version) do
    args = [{:short, rid.cluster_id},
            {:long, rid.position},
            {:int, version},
            {:raw, <<0>>}]

    C.operation(conn, :record_delete, args)
  end

  @doc """
  Execute the given `query` in the database to which `conn` is connected.

  `opts` is a list of options that depend on the kind of command being issued.

  If `query` is an *idempotent* command (`SELECT`), then the options are:

    * `:fetch_plan`: a string specifying the fetch plan. Mandatory for `SELECT`
      queries.

  """
  @spec command(pid, String.t, Keyword.t) :: term
  def command(conn, query, opts) do
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

  defp encode_query_with_type(:sql_query, query, opts) do
    params = opts[:params] || %{}
    args = [query,
            -1,
            Keyword.fetch!(opts, :fetch_plan),
            %Record{class: nil, fields: %{"params" => params}}]

    Enum.map(args, &Protocol.encode_term/1)
  end

  defp encode_query_with_type(:sql_command, query, opts) do
    args = [query]

    if params = opts[:params] do
      params = %Record{class: nil, fields: %{"parameters" => params}}
      # `true` means "use simple parameters".
      args = args ++ [true, params]
    else
      args = args ++ [false]
    end

    args = args ++ [false]
    Enum.map(args, &Protocol.encode_term/1)
  end
end
