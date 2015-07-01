defmodule MarcoPolo do
  alias MarcoPolo.Connection, as: C

  @default_opts [
    host: "localhost",
    port: 2424,
  ]

  @doc """
  """
  @spec start_link(Keyword.t) :: GenServer.on_start
  def start_link(opts \\ []) do
    unless Keyword.get(opts, :connection) do
      raise ArgumentError, "no connection type (connect/db_open) specified"
    end

    Connection.start_link(C, Keyword.merge(@default_opts, opts))
  end

  def db_exists?(conn, db, db_type) do
    Connection.call(conn, {:operation, :db_exist, [db, db_type]})
  end

  def db_reload(conn) do
    case Connection.call(conn, {:operation, :db_reload, []}) do
      {:ok, _}            -> :ok
      {:error, _} = error -> error
    end
  end

  def db_size(conn) do
    Connection.call(conn, {:operation, :db_size, []})
  end

  def db_countrecords(conn) do
    Connection.call(conn, {:operation, :db_countrecords, []})
  end
end
