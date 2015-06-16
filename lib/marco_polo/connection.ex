defmodule MarcoPolo.Connection do
  use Connection

  require Logger

  alias MarcoPolo.Protocol
  alias MarcoPolo.Error
  import MarcoPolo.Protocol.BinaryHelpers

  @protocol 28

  @connection_args [
    "OrientDB binary driver for Elixir",
    "0.0.1-beta",
    {:short, @protocol},
    "client id",
    "ORecordSerializerBinary",
  ]

  @socket_opts [:binary, active: false, packet: :raw]

  @default_opts [
    host: "localhost",
    port: 2424,
    token?: false,
  ]

  @initial_state %{socket: nil,
                   session_id: nil,
                   token: nil,
                   queue: :queue.new}

  ## Client code.

  @doc """
  """
  @spec start_link(Keyword.t) :: GenServer.on_start
  def start_link(opts \\ []) do
    opts = Keyword.merge(@default_opts, opts)
    Connection.start_link(__MODULE__, opts)
  end

  def operation(pid, op_name, args) do
    Connection.call(pid, {:operation, op_name, args})
  end

  ## Callbacks.

  @doc false
  def init(opts) do
    s = Dict.merge(@initial_state, opts: opts)
    {:connect, :init, s}
  end

  @doc false
  def connect(_info, s) do
    {host, port, socket_opts} = tcp_connection_opts(s)

    case :gen_tcp.connect(host, port, socket_opts) do
      {:ok, socket} ->
        s = %{s | socket: socket}
        case do_connect(s) do
          {:ok, s} ->
            :inet.setopts(socket, active: :once)
            {:ok, s}
          %Error{} = error ->
            {:stop, error, s}
          {:tcp_error, reason} ->
            {:stop, reason, s}
        end
      {:error, reason} ->
        Logger.error "OrientDB TCP connect error (#{host}:#{port}): #{:inet.format_error(reason)}"
        {:stop, reason, s}
    end
  end

  @doc false
  def handle_call(call, from, s)

  def handle_call({:operation, op_name, args}, from, %{session_id: sid} = s) do
    args = if token = s.token, do: [sid, token|args], else: [sid|args]
    req  = Protocol.encode_op(op_name, args)

    s = update_in(s.queue, &:queue.in({from, op_name}, &1))

    :gen_tcp.send(s.socket, req)
    {:noreply, s}
  end

  @doc false
  def handle_info(msg, state)

  def handle_info({:tcp, socket, msg}, %{session_id: sid} = s) do
    # Reactivate the socket.
    :inet.setopts(s.socket, active: :once)

    {{:value, {from, op_name}}, new_queue} = :queue.out(s.queue)
    s = %{s | queue: new_queue}

    resp = case Protocol.parse_resp(op_name, msg, s.opts[:token?]) do
      {:ok, ^sid, resp}         -> resp
      {:ok, ^sid, _token, resp} -> resp
    end

    Connection.reply(from, resp)
    {:noreply, s}
  end

  def handle_info({:tcp_closed, socket}, %{socket: socket} = s) do
    Logger.error "TCP closed"
    {:noreply, s}
  end

  def handle_info(msg, s) do
    IO.puts "Received unhandled message: #{inspect msg}"
    {:noreply, s}
  end

  # Helper functions.

  defp tcp_connection_opts(%{opts: opts} = _state) do
    socket_opts = @socket_opts ++ (opts[:socket_opts] || [])
    {to_char_list(opts[:host]), opts[:port], socket_opts}
  end

  defp do_connect(%{socket: socket} = s) do
    case negotiate_protocol(socket) do
      :ok                     -> authenticate(s)
      %Error{} = error        -> error
      {:tcp_error, _} = error -> error
    end
  end

  defp authenticate(%{opts: opts, socket: socket} = s) do
    user     = Keyword.fetch!(opts, :user)
    password = Keyword.fetch!(opts, :password)
    token?   = Keyword.fetch!(opts, :token?)

    req = case Keyword.fetch!(opts, :connection) do
      :server ->
        Protocol.encode_op(:connect, [nil] ++ @connection_args ++ [token?, user, password])
      {:db, db_name, db_type} ->
        Protocol.encode_op(:db_open, [nil] ++ @connection_args ++ [token?, db_name, db_type, user, password])
    end

    case :gen_tcp.send(socket, req) do
      :ok ->
        case :gen_tcp.recv(socket, 0) do
          {:ok, data}      -> parse_connection_response(data, s)
          {:error, reason} -> {:tcp_error, reason}
        end
      {:error, reason} ->
        {:tcp_error, reason}
    end
  end

  defp parse_connection_response(data, s) do
    case Protocol.parse_connection_header(data) do
      {:ok, sid, rest} ->
        {token, _rest} = Protocol.parse(rest, :bytes)

        s = %{s | session_id: sid}
        s = %{s | token: nullify_empty(token)}
        {:ok, s}
      %Error{} = error ->
        error
    end
  end

  defp negotiate_protocol(socket) do
    case :gen_tcp.recv(socket, 2) do
      {:ok, <<protocol_number :: short>>} ->
        check_protocol_number(protocol_number)
      {:error, reason} ->
        {:tcp_error, reason}
    end
  end

  defp check_protocol_number(protocol_number) do
    if protocol_number >= Application.get_env(:marco_polo, :supported_protocol) do
      :ok
    else
      %Error{message: "unsupported protocol version, the supported version is >= #{protocol_number}"}
    end
  end

  defp nullify_empty(""),   do: nil
  defp nullify_empty(term), do: term
end
