defmodule MarcoPolo.Connection.Auth do
  @moduledoc false

  import MarcoPolo.Protocol.BinaryHelpers
  alias MarcoPolo.Protocol

  @typep state :: Map.t

  @min_protocol 28
  @protocol 30

  @timeout 5000

  @connection_args [
    "OrientDB binary driver for Elixir", # client name
    "0.0.1-beta",                        # client version
    {:short, @protocol},                 # protocol number
    "client id",
    "ORecordSerializerBinary",           # serialization protocol
    false,                               # token-based auth, not supported
  ]

  @doc """
  Authenticate to the OrientDB server to perform server or database operations.

  The type of connection (ultimately distinguishing between REQUEST_CONNECT and
  REQUEST_DB_OPEN) is stored in `opts[:connection]`, which is required.
  """
  @spec connect(state) :: {:ok, state} | {:error, term, state} | {:tcp_error, term, state}
  def connect(s) do
    case negotiate_protocol(s) do
      {:ok, s} ->
        authenticate(s)
      {:tcp_error, reason} ->
        {:tcp_error, reason, s}
    end
  end

  defp negotiate_protocol(%{socket: socket, opts: opts} = s) do
    case :gen_tcp.recv(socket, 2, opts[:timeout] || @timeout) do
      {:ok, <<version :: short>>} ->
        check_min_protocol!(version)
        {:ok, %{s | protocol_version: version}}
      {:error, reason} ->
        {:tcp_error, reason}
    end
  end

  defp authenticate(%{opts: opts, socket: socket} = s) do
    user     = Keyword.fetch!(opts, :user)
    password = Keyword.fetch!(opts, :password)

    {op, args} = op_and_args_from_connection_type(user, password, Keyword.fetch!(opts, :connection))

    # The first `nil` is for the session id, that is required to be nil (-1) for
    # first-time connections.
    req = Protocol.encode_op(op, [nil|@connection_args] ++ args)

    case :gen_tcp.send(socket, req) do
      :ok              -> wait_for_connection_response(s, op)
      {:error, reason} -> {:tcp_error, reason, s}
    end
  end

  defp op_and_args_from_connection_type(user, password, :server),
    do: {:connect, [user, password]}
  defp op_and_args_from_connection_type(user, password, {:db, name, type})
    when type in [:document, :graph],
    do: {:db_open, [name, Atom.to_string(type), user, password]}
  defp op_and_args_from_connection_type(_user, _password, {:db, _, type}),
    do: raise(ArgumentError, "unknown database type: #{inspect type}, valid ones are :document, :graph")
  defp op_and_args_from_connection_type(_user, _password, _),
    do: raise(ArgumentError, "invalid connection type, valid ones are :server or {:db, name, type}")

  defp wait_for_connection_response(%{socket: socket, opts: opts} = s, connection_type) do
    case :gen_tcp.recv(socket, 0, opts[:timeout] || @timeout) do
      {:error, reason} ->
        {:tcp_error, reason, s}
      {:ok, new_data} ->
        data = s.tail <> new_data
        case Protocol.parse_connection_resp(data, connection_type) do
          :incomplete ->
            wait_for_connection_response(%{s | tail: data}, connection_type)
          {-1, {:error, err}, rest} ->
            {:error, err, %{s | tail: rest}}
          {-1, {:ok, [sid, _token]}, rest} ->
            {:ok, %{s | session_id: sid, tail: rest}}
        end
    end
  end

  defp check_min_protocol!(protocol) when protocol < @min_protocol do
    raise Error, """
    the minimum supported protocol is #{@min_protocol}, the server is using #{protocol}
    """
  end

  defp check_min_protocol!(_) do
    :ok
  end
end
