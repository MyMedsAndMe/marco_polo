defmodule MarcoPolo.Connection do
  @moduledoc false

  use Connection

  require Logger

  alias MarcoPolo.Connection.Auth
  alias MarcoPolo.Connection.LiveQuery
  alias MarcoPolo.Protocol
  alias MarcoPolo.Document
  alias MarcoPolo.Error

  @type state :: %{}

  @socket_opts [:binary, active: false, packet: :raw]

  @timeout 5000

  @initial_state %{
    # The TCP socket to the OrientDB server
    socket: nil,
    # The module used to connect to the server (:gen_tcp or :ssl)
    socket_module: nil,
    # The session id for the session held by this genserver
    session_id: nil,
    # The queue of commands sent to the server
    queue: :queue.new,
    # The schema of the OrientDB database (if we're connected to a db)
    schema: nil,
    # The tail of binary data from parsing
    tail: "",
    # A monothonically increasing transaction id (must be unique per session)
    transaction_id: 1,
    # The options used to start this genserver
    opts: nil,
    # The protocol (version) that the server this genserver is connected to is
    # using
    protocol_version: nil,
    # Dict of live query tokens to receiver pids
    live_query_tokens: HashDict.new,
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
      {:ok, pid} = res ->
        maybe_fetch_schema(pid, opts)
        res
      {:error, _} = err ->
        err
    end
  end

  @doc """
  Shuts down the connection (asynchronously since it's a cast).
  """
  @spec stop(pid) :: :ok
  def stop(pid) do
    Connection.cast(pid, :stop)
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

  def live_query(pid, args, receiver, opts) do
    Connection.call(pid, {:live_query, args, receiver}, opts[:timeout] || @timeout)
  end

  def live_query_unsubscribe(pid, token) do
    Connection.cast(pid, {:live_query_unsubscribe, token})
  end

  @doc """
  Fetch the schema and store it into the state.

  Always returns `:ok` without waiting for the schema to be fetched.
  """
  @spec fetch_schema(pid) :: Dict.t
  def fetch_schema(pid) do
    Connection.call(pid, :fetch_schema)
  end

  defp maybe_fetch_schema(pid, opts) do
    case Keyword.get(opts, :connection) do
      {:db, _} -> fetch_schema(pid)
      _           -> nil
    end
  end

  ## Callbacks.

  @doc false
  def init(opts) do
    s = Dict.merge(@initial_state, opts: opts)
    s = %{s | socket_module: (if opts[:ssl], do: :ssl, else: :gen_tcp)}
    {:connect, :init, s}
  end

  @doc false
  def connect(_info, %{opts: opts} = s) do
    case connect_over_socket(s) do
      {:ok, socket} ->
        s = %{s | socket: socket}
        setup_socket_buffers(s)

        case Auth.connect(s) do
          {:ok, s} ->
            inet_module(s).setopts(socket, active: :once)
            {:ok, s}
          {:error, error, s} ->
            {:stop, error, s}
          {:tcp_error, reason, s} ->
            {:stop, reason, s}
        end
      {:error, reason} ->
        Logger.error ["OrientDB connect error (#{opts[:host]}:#{opts[:port]}): ",
                      inet_module(s).format_error(reason)]
        {:stop, reason, s}
    end
  end

  @doc false
  def disconnect(:stop, s) do
    {:stop, :normal, s}
  end

  def disconnect(error, s) do
    # We only care about {from, _} tuples, ignoring queued stuff like
    # :fetch_schema.
    for {from, _operation} <- :queue.to_list(s.queue) do
      Connection.reply(from, error)
    end

    # Backoff 0 to churn through all commands in mailbox before reconnecting,
    # https://github.com/ericmj/mongodb/blob/a2dba1dfc089960d87364c2c43892f3061a93924/lib/mongo/connection.ex#L210
    {:backoff, 0, %{s | socket: nil, queue: :queue.new, transaction_id: 1}}
  end

  @doc false
  def handle_call(op, from, s)

  # No socket means there's no TCP connection, we can return an error to the
  # client.
  def handle_call(_call, _from, %{socket: nil} = s) do
    {:reply, {:error, :closed}, s}
  end

  # We have to handle the :tx_commit operation differently as we have to keep
  # track of the transaction id, which is kept in the state of this genserver
  # (we also have to update this id).
  def handle_call({:operation, :tx_commit, [:transaction_id|args]}, from, s) do
    {id, s} = next_transaction_id(s)
    handle_call({:operation, :tx_commit, [id|args]}, from, s)
  end

  def handle_call({:operation, op_name, args}, from, %{session_id: sid} = s) do
    check_op_is_allowed!(s, op_name)
    check_op_with_version!(s.protocol_version, op_name)

    s
    |> enqueue({from, op_name})
    |> send_noreply(Protocol.encode_op(op_name, [sid|args]))
  end

  def handle_call(:fetch_schema, from, %{session_id: sid} = s) do
    check_op_is_allowed!(s, :record_load)

    args = [sid, {:short, 0}, {:long, 1}, "*:-1", true, false]

    s
    |> enqueue({:fetch_schema, from})
    |> send_noreply(Protocol.encode_op(:record_load, args))
  end

  def handle_call({:live_query, args, receiver}, from, s) do
    check_op_is_allowed!(s, :command)

    s
    |> enqueue({:live_query, from, receiver})
    |> send_noreply(Protocol.encode_op(:command, [s.session_id|args]))
  end

  @doc false
  def handle_cast(op, s)

  def handle_cast({:operation, op_name, args}, %{session_id: sid} = s) do
    check_op_is_allowed!(s, op_name)

    send_noreply(s, Protocol.encode_op(op_name, [sid|args]))
  end

  def handle_cast(:stop, s) do
    {:disconnect, :stop, s}
  end

  def handle_cast({:live_query_unsubscribe, token}, s) do
    s = update_in(s.live_query_tokens, &Dict.delete(&1, token))
    {:noreply, s}
  end

  @doc false
  def handle_info(msg, s)

  def handle_info({type, socket, msg}, %{socket: socket} = s)
  when type in [:tcp, :ssl] do
    inet_module(s).setopts(socket, active: :once)
    data = s.tail <> msg

    s =
      if Protocol.live_query_data?(data) do
        LiveQuery.forward_live_query_data(data, s)
      else
        dequeue_and_parse_resp(s, :queue.out(s.queue), data)
      end

    {:noreply, s}
  end

  def handle_info({type, socket}, %{socket: socket} = s)
  when type in [:tcp_closed, :ssl_closed] do
    {:disconnect, {:error, :closed}, s}
  end

  def handle_info({type, socket, reason}, %{socket: socket} = s)
  when type in [:tcp_error, :ssl_error] do
    {:disconnect, {:error, reason}, s}
  end

  # Helper functions.

  defp connect_over_socket(%{opts: opts} = s) do
    socket_opts = @socket_opts ++ (opts[:socket_opts] || [])
    connection_opts =
      if opts[:ssl] do
        socket_opts ++ (opts[:ssl_opts] || [])
      else
        socket_opts
      end

    s.socket_module.connect(to_char_list(opts[:host]),
                            opts[:port],
                            connection_opts,
                            opts[:timeout] || @timeout)
  end

  defp parse_schema(%Document{fields: %{"globalProperties" => properties}}) do
    global_properties =
      for %Document{fields: %{"name" => name, "type" => type, "id" => id}} <- properties,
        into: HashDict.new() do
          {id, {name, type}}
      end

    %{global_properties: global_properties}
  end

  defp setup_socket_buffers(%{socket: socket} = s) do
    {:ok, info} = inet_module(s).getopts(socket, [:sndbuf, :recbuf, :buffer])
    buffer = info[:buffer] |> max(info[:sndbuf]) |> max(info[:recbuf])
    :ok = inet_module(s).setopts(socket, [buffer: buffer])
  end

  defp inet_module(%{socket_module: :gen_tcp}), do: :inet
  defp inet_module(%{socket_module: :ssl}),     do: :ssl

  defp send_noreply(%{socket: socket} = s, req) do
    case s.socket_module.send(socket, req) do
      :ok                       -> {:noreply, s}
      {:error, _reason} = error -> {:disconnect, error, s}
    end
  end

  defp enqueue(s, what) do
    update_in(s.queue, &:queue.in(what, &1))
  end

  # We handle some cases of stuff in the queue differently as we have to do
  # stuff to the state (e.g., storing the schema in the state).
  defp dequeue_and_parse_resp(s, popped_from_queue, data)

  defp dequeue_and_parse_resp(s, {{:value, {:fetch_schema, from}}, new_queue}, data) do
    sid = s.session_id

    # Fetching the schema is a REQUEST_RECORD_LOAD operation.
    case Protocol.parse_resp(:record_load, data, s.schema) do
      :incomplete ->
        %{s | tail: data}
      {^sid, {:error, reason}, _rest} ->
        raise Error, "couldn't fetch the schema because: #{inspect reason}"
      {^sid, {:ok, {schema, _linked_records}}, rest} ->
        schema = parse_schema(schema)
        Connection.reply(from, schema)
        s = %{s | schema: schema, queue: new_queue}
        dequeue_and_parse_resp(s, :queue.out(new_queue), rest)
    end
  end

  defp dequeue_and_parse_resp(s, {{:value, {:live_query, from, receiver}}, new_queue}, data) do
    sid = s.session_id

    # A live query is just a command (e.g., "LIVE SELECT ..."), so we parse it
    # as such.
    case Protocol.parse_resp(:command, data, s.schema) do
      :incomplete ->
        %{s | tail: data}
      {^sid, {:ok, resp}, rest} ->
        token = LiveQuery.extract_token(resp)
        Connection.reply(from, {:ok, token})
        s =
          s
          |> Map.put(:tail, rest)
          |> Map.put(:queue, new_queue)
          |> put_in([:live_query_tokens, token], receiver)
        dequeue_and_parse_resp(s, :queue.out(new_queue), rest)
    end
  end

  defp dequeue_and_parse_resp(s, {{:value, {from, op_name}}, new_queue}, data) do
    sid = s.session_id

    case Protocol.parse_resp(op_name, data, s.schema) do
      :incomplete ->
        %{s | tail: data}
      {^sid, resp, rest} ->
        Connection.reply(from, resp)
        s = %{s | queue: new_queue}
        dequeue_and_parse_resp(s, :queue.out(new_queue), rest)
    end
  end

  defp dequeue_and_parse_resp(s, {:empty, queue}, "") do
    %{s | queue: queue}
  end

  defp check_op_is_allowed!(%{opts: opts}, operation) do
    do_check_op_is_allowed!(Keyword.fetch!(opts, :connection), operation)
  end

  @server_ops ~w(
    shutdown
    db_create
    db_exist
    db_drop
  )a

  @db_ops ~w(
    db_close
    db_size
    db_countrecords
    db_reload
    record_load
    record_load_if_version_not_latest
    record_create
    record_update
    record_delete
    command
    tx_commit
  )a

  defp do_check_op_is_allowed!({:db, _}, op) when not op in @db_ops do
    raise Error, "must be connected to the server (not a db) to perform operation #{op}"
  end

  defp do_check_op_is_allowed!(:server, op) when not op in @server_ops do
    raise Error, "must be connected to a database to perform operation #{op}"
  end

  defp do_check_op_is_allowed!(_, _) do
    nil
  end

  @ops_with_versions %{
    record_load_if_version_not_latest: 30,
  }

  for {op, min_version} <- @ops_with_versions do
    defp check_op_with_version!(current, unquote(op)) when current < unquote(min_version) do
      raise MarcoPolo.VersionError,
        "operation #{unquote(op)} is not supported in" <>
        " the current version of the OrientDB binary protocol," <>
        " (#{current}), only starting with version #{unquote(min_version)}"
    end
  end

  defp check_op_with_version!(_current, _op) do
    :ok
  end

  defp next_transaction_id(s) do
    get_and_update_in(s.transaction_id, &{&1, &1 + 1})
  end
end
