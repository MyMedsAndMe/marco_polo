defmodule MarcoPolo do
  alias MarcoPolo.Connection, as: C
  alias MarcoPolo.RID
  alias MarcoPolo.Record

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
    unless Keyword.get(opts, :connection) do
      raise ArgumentError, "no connection type (connect/db_open) specified"
    end

    Connection.start_link(C, Keyword.merge(@default_opts, opts))
  end

  @doc """
  Tells whether the database called `name` with the given `type` exists.

  ## Examples

      iex> MarcoPolo.db_exists?(conn, "GratefulDeadConcerts", "plocal")
      {:ok, true}

  """
  @spec db_exists?(pid, String.t, String.t) :: {:ok, boolean}
  def db_exists?(conn, name, type) do
    Connection.call(conn, {:operation, :db_exist, [name, type]})
  end

  @doc """
  Reloads the database to which `conn` is connected.

  ## Examples

      iex> MarcoPolo.db_reload(conn)
      :ok

  """
  @spec db_reload(pid) :: :ok
  def db_reload(conn) do
    case Connection.call(conn, {:operation, :db_reload, []}) do
      {:ok, _}            -> :ok
      {:error, _} = error -> error
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
    Connection.call(conn, {:operation, :db_size, []})
  end

  @doc """
  Returns the number of records in the database to which `conn` is connected.

  ## Examples

      iex> MarcoPolo.db_countrecords(conn)
      {:ok, 7931}

  """
  @spec db_countrecords(pid) :: {:ok, non_neg_integer}
  def db_countrecords(conn) do
    Connection.call(conn, {:operation, :db_countrecords, []})
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

    case Connection.call(conn, {:operation, :record_create, args}) do
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

  ## Examples

      iex> rid = %MarcoPolo.RID{cluster_id: 10, position: 184}
      iex> {:ok, [record]} = MarcoPolo.load_record(conn, rid, "*:-1")
      iex> record.fields
      %{"foo" => "bar"}

  """
  @spec load_record(pid, RID.t, String.t, Keyword.t) :: {:ok, [Record.t]}
  def load_record(conn, %RID{} = rid, fetch_plan, opts \\ []) do
    args = [{:short, rid.cluster_id},
            {:long, rid.position},
            fetch_plan,
            opts[:ignore_cache] || true,
            opts[:load_tombstones] || false]

    Connection.call(conn, {:operation, :record_load, args})
  end
end
