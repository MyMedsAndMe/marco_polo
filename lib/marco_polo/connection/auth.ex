defmodule MarcoPolo.Connection.Auth do
  @moduledoc false

  import MarcoPolo.Protocol.BinaryHelpers
  alias MarcoPolo.Protocol

  @typep state :: Map.t

  @min_protocol 28

  @serialization_protocol "ORecordSerializerBinary"

  @timeout 5000

  @doc """
  Authenticate to the OrientDB server to perform server or database operations.

  The type of connection (either a `REQUEST_CONNECT` or a `REQUEST_DB_OPEN`) is
   stored in `opts[:connection]`. `:connection` is a required key in the options.
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

  # Waits for the 2 byte protocol version, checks that the protocol is supported
  # and stores it in the state.
  defp negotiate_protocol(%{socket: socket, opts: opts} = s) do
    case s.socket_module.recv(socket, 2, opts[:timeout] || @timeout) do
      {:ok, <<version :: short>>} ->
        check_min_protocol!(version)
        {:ok, %{s | protocol_version: version}}
      {:error, reason} ->
        {:tcp_error, reason}
    end
  end

  # Sends the authentication data (based on the `opts` in the state) over the
  # wire and waits for a response.
  defp authenticate(%{socket: socket} = s) do
    {op, args} = op_and_connection_args(s)
    req = Protocol.encode_op(op, args)

    case s.socket_module.send(socket, req) do
      :ok ->
        wait_for_connection_response(s, op)
      {:error, reason} ->
        {:tcp_error, reason, s}
    end
  end

  defp op_and_connection_args(%{opts: opts, protocol_version: protocol}) do
    {op, other_args} = op_and_args_from_connection_type(Keyword.fetch!(opts, :connection))

    static_args = [
      nil, # session id, nil (-1) for first-time connections
      Application.get_env(:marco_polo, :client_name),
      Application.get_env(:marco_polo, :version),
      {:short, protocol},
      "client id",
      @serialization_protocol,
      false, # token-based auth, not supported
    ]

    user = Keyword.fetch!(opts, :user)
    password = Keyword.fetch!(opts, :password)

    {op, static_args ++ other_args ++ [user, password]}
  end

  defp op_and_args_from_connection_type(:server),
    do: {:connect, []}
  defp op_and_args_from_connection_type({:db, name}),
    do: {:db_open, [name]}
  defp op_and_args_from_connection_type({:db, _name, _type}),
    do: raise(ArgumentError, "the database type is not supported (anymore) when connecting to a database, use {:db, db_name} instead")
  defp op_and_args_from_connection_type(_type),
    do: raise(ArgumentError, "invalid connection type, valid ones are :server or {:db, name}")

  defp wait_for_connection_response(%{socket: socket, opts: opts} = s, connection_type) do
    case s.socket_module.recv(socket, 0, opts[:timeout] || @timeout) do
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
