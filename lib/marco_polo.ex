defmodule MarcoPolo do
  alias MarcoPolo.Connection, as: C

  @default_opts [
    host: "localhost",
    port: 2424,
    token?: false,
  ]

  @spec start_link(Keyword.t) :: GenServer.on_start
  def start_link(opts \\ []) do
    opts = Keyword.merge(@default_opts, opts)
    Connection.start_link(C, opts)
  end
end
