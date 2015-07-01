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
end
