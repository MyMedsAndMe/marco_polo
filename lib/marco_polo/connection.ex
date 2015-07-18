defmodule MarcoPolo.Connection do
  @moduledoc false

  use Connection

  require Logger

  alias MarcoPolo.Connection.Auth
  alias MarcoPolo.Protocol
  alias MarcoPolo.Document
  alias MarcoPolo.Error

  @socket_opts [:binary, active: false, packet: :raw]

  @timeout 5000

  @initial_state %{
    socket: nil,
    session_id: nil,
    queue: :queue.new,
    schema: nil,
    tail: "",
  }

  ## Client code.

  @doc """
  Starts the current `Connection`. If the (successful) connection is to a
  database, fetch the schema.
  """
  @spec start_link(Keyword.t) :: GenServer.on_start
  def start_link(opts) do
    # The first `opts` is the value to pass to the `init/1` callback, the second
    # one is the list of options being passed to `Connection.start_link` (e.g.,
    # `:name` or `:timeout`).
    case Connection.start_link(__MODULE__, opts, opts) do
      {:error, _} = err ->
        err
      {:ok, pid} = res ->
        maybe_fetch_schema(pid, opts)
        res
    end
  end

  @doc """
  Performs the operation identified by `op_name` with the connection on
  `pid`. `args` is the list of arguments to pass to the operation.
  """
  @spec operation(pid, atom, [Protocol.encodable_term], Keyword.t) ::
    {:ok, term} | {:error, term}
  def operation(pid, op_name, args, opts) do
    Connection.call(pid, {:operation, op_name, args}, opts[:timeout] || @timeout)
  end

  @doc """
  Does what `operation/3` does but expects no response from OrientDB and always
  returns `:ok`.
  """
  @spec no_response_operation(pid, atom, [Protocol.encodable_term]) :: :ok
  def no_response_operation(pid, op_name, args) do
    Connection.cast(pid, {:operation, op_name, args})
  end

  @doc """
  Fetch the schema and store it into the state.

  Always returns `:ok` without waiting for the schema to be fetched.
  """
  @spec fetch_schema(pid) :: :ok
  def fetch_schema(pid) do
    Connection.cast(pid, :fetch_schema)
  end

  defp maybe_fetch_schema(pid, opts) do
    case Keyword.get(opts, :connection) do
      {:db, _, _} -> fetch_schema(pid)
      _           -> nil
    end
  end

  ## Callbacks.

  @doc false
  def init(opts) do
    s = Dict.merge(@initial_state, opts: opts)
    {:connect, :init, s}
  end

  @doc false
  def connect(_info, s) do
    {host, port, socket_opts, timeout} = tcp_connection_opts(s)

    case :gen_tcp.connect(host, port, socket_opts, timeout) do
      {:ok, socket} ->
        s = %{s | socket: socket}
        setup_socket_buffers(socket)

        case Auth.connect(s) do
          {:ok, s} ->
            :inet.setopts(socket, active: :once)
            {:ok, s}
          {:error, error, s} ->
            {:stop, error, s}
          {:tcp_error, reason, s} ->
            {:stop, reason, s}
        end
      {:error, reason} ->
        Logger.error "OrientDB TCP connect error (#{host}:#{port}): #{:inet.format_error(reason)}"
        {:stop, reason, s}
    end
  end

  @doc false
  def disconnect(error, s) do
    # We only care about {from, _} tuples, ignoring queued stuff like
    # :fetch_schema.
    for {from, _operation} <- :queue.to_list(s.queue) do
      Connection.reply(from, error)
    end

    # Backoff 0 to churn through all commands in mailbox before reconnecting,
    # https://github.com/ericmj/mongodb/blob/a2dba1dfc089960d87364c2c43892f3061a93924/lib/mongo/connection.ex#L210
    {:backoff, 0, %{s | socket: nil, queue: :queue.new}}
  end

  @doc false
  # No socket means there's no TCP connection, we can return an error to the
  # client.
  def handle_call(_call, _from, %{socket: nil} = s) do
    {:reply, {:error, :closed}, s}
  end

  def handle_call({:operation, op_name, args}, from, %{session_id: sid} = s) do
    req = Protocol.encode_op(op_name, [sid|args])
    s
    |> enqueue({from, op_name})
    |> send_noreply(req)
  end

  @doc false
  def handle_cast({:operation, op_name, args}, %{session_id: sid} = s) do
    req = Protocol.encode_op(op_name, [sid|args])
    send_noreply(s, req)
  end

  def handle_cast(:fetch_schema, %{session_id: sid} = s) do
    args = [sid, {:short, 0}, {:long, 1}, "*:-1", true, false]
    req = Protocol.encode_op(:record_load, args)

    s
    |> enqueue(:fetch_schema)
    |> send_noreply(req)
  end

  @doc false
  def handle_info({:tcp, socket, msg}, %{socket: socket} = s) do
    :inet.setopts(socket, active: :once)
    s = dequeue_and_parse_resp(s, :queue.out(s.queue), s.tail <> msg)
    {:noreply, s}
  end

  def handle_info({:tcp_closed, socket}, %{socket: socket} = s) do
    {:disconnect, {:error, :closed}, s}
  end

  # Helper functions.

  defp tcp_connection_opts(%{opts: opts} = _state) do
    socket_opts = @socket_opts ++ (opts[:socket_opts] || [])
    {to_char_list(opts[:host]), opts[:port], socket_opts, opts[:timeout] || @timeout}
  end

  defp parse_schema(%Document{fields: %{"globalProperties" => properties}}) do
    global_properties =
      for %Document{fields: %{"name" => name, "type" => type, "id" => id}} <- properties,
        into: HashDict.new() do
          {id, {name, type}}
      end

    %{global_properties: global_properties}
  end

  defp setup_socket_buffers(socket) do
    {:ok, [sndbuf: sndbuf, recbuf: recbuf]} = :inet.getopts(socket, [:sndbuf, :recbuf])
    :ok = :inet.setopts(socket, [buffer: max(sndbuf, recbuf)])
  end

  defp send_noreply(%{socket: socket} = s, req) do
    case :gen_tcp.send(socket, req) do
      :ok                       -> {:noreply, s}
      {:error, _reason} = error -> {:disconnect, error, s}
    end
  end

  defp enqueue(s, what) do
    update_in s.queue, &:queue.in(what, &1)
  end

  defp dequeue_and_parse_resp(s, {{:value, :fetch_schema}, new_queue}, data) do
    sid = s.session_id

    case Protocol.parse_resp(:record_load, data, s.schema) do
      :incomplete ->
        %{s | tail: data}
      {^sid, {:error, _}, _rest} ->
        raise "couldn't fetch schema"
      {^sid, {:ok, [schema]}, rest} ->
        %{s | schema: parse_schema(schema), tail: rest, queue: new_queue}
    end
  end

  defp dequeue_and_parse_resp(s, {{:value, {from, op_name}}, new_queue}, data) do
    sid = s.session_id

    case Protocol.parse_resp(op_name, data, s.schema) do
      :incomplete ->
        %{s | tail: data}
      {^sid, resp, rest} ->
        Connection.reply(from, resp)
        %{s | tail: rest, queue: new_queue}
    end
  end
end
