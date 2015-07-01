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
end
