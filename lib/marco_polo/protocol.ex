defmodule MarcoPolo.Protocol do
  @moduledoc false

  require Integer

  import MarcoPolo.Protocol.BinaryHelpers

  alias MarcoPolo.GenericParser, as: GP
  alias MarcoPolo.Error
  alias MarcoPolo.Document
  alias MarcoPolo.UndecodedDocument
  alias MarcoPolo.BinaryRecord
  alias MarcoPolo.RID
  alias MarcoPolo.Protocol.RecordSerialization
  alias MarcoPolo.Protocol.CSVTypes

  @type encodable_term ::
    boolean
    | nil
    | binary
    | integer
    | iolist
    | {:short, integer}
    | {:int, integer}
    | {:long, integer}
    | {:raw, binary}
    | Document.t
    | BinaryRecord.t

  @ok        <<0>>
  @error     <<1>>
  @push_data <<3>>

  @document ?d
  @binary_record ?b

  @doc """
  Encodes an operation given its name (`op_name`) and a list of arguments
  (`args`).
  """
  @spec encode_op(atom, [encodable_term]) :: iodata
  def encode_op(op_name, args) do
    [req_code(op_name), encode_list_of_terms(args)]
  end

  @doc """
  Encdes a list of terms.
  """
  @spec encode_list_of_terms([encodable_term]) :: iodata
  def encode_list_of_terms(list) when is_list(list) do
    Enum.map list, &encode_term/1
  end

  @doc """
  Encodes a given term according to the binary protocol.

  The type of `term` is usually inferred by its value but in some cases it can
  be specified by using a tagged tuple. For example, to force encodng of an
  integer as an OrientDB short, you can pass `{:short, n}`.
  """
  # Made public for testing.
  @spec encode_term(encodable_term) :: iodata
  def encode_term(term)

  # Booleans.
  def encode_term(true),  do: <<1>>
  def encode_term(false), do: <<0>>

  # nil.
  def encode_term(nil), do: encode_term({:int, -1})

  # Strings and bytes.
  def encode_term(str) when is_binary(str), do: encode_term({:int, byte_size(str)}) <> str

  # Encoding an Elixir integer defaults to encoding an OrientDB int (4 bytes).
  def encode_term(i) when is_integer(i), do: encode_term({:int, i})

  # Typed integers (short, int and long) have to be tagged.
  def encode_term({:short, i}), do: <<i :: short>>
  def encode_term({:int, i}),   do: <<i :: int>>
  def encode_term({:long, i}),  do: <<i :: long>>

  # A list is assumed to be iodata and is converted to binary before being serialized.
  def encode_term(data) when is_list(data), do: [encode_term(IO.iodata_length(data)), data]

  # Raw bytes (that have no leading length, just the bytes).
  def encode_term({:raw, bytes}) when is_binary(bytes) or is_list(bytes), do: bytes

  # An entire document.
  def encode_term(%Document{} = record), do: encode_term(RecordSerialization.encode(record))

  # A binary record (BLOB).
  def encode_term(%BinaryRecord{contents: bytes}), do: encode_term(bytes)

  @doc """
  Decodes an instance of `type` from `data`.

  Returns a `{value, rest}` tuple or the `:incomplete` atom if `data` doesn't
  contain a full instance of `type`.
  """
  @spec decode_term(binary, atom) :: {term, binary} | :incomplete
  def decode_term(data, type)

  def decode_term(<<-1 :: int, rest :: binary>>, type) when type in [:string, :bytes] do
    {nil, rest}
  end

  def decode_term(<<length :: int, data :: binary>>, type) when type in [:string, :bytes] do
    case data do
      <<parsed :: bytes-size(length), rest :: binary>> -> {parsed, rest}
      _                                                -> :incomplete
    end
  end

  def decode_term(<<byte, rest :: binary>>, :byte), do: {byte, rest}

  def decode_term(<<i :: short, rest :: binary>>, :short), do: {i, rest}
  def decode_term(<<i :: int, rest :: binary>>, :int),     do: {i, rest}
  def decode_term(<<i :: long, rest :: binary>>, :long),   do: {i, rest}

  def decode_term(_data, _type) do
    :incomplete
  end

  def live_query_data?(@push_data <> _), do: true
  def live_query_data?(_), do: false

  @doc """
  Parses the response to a given operation.

  `op_name` (the name of the operation as an atom, like `:record_load`) is used
  to determine the structure of the response. `schema` is passed down to the
  record deserialization (in case there are records) in order to decode possible
  property ids.

  The return value is a three-element tuple:

    * the first element is the session id of the response.
    * the second element is the actual response (which can be `{:ok, something}`
      or `{:error, reason}`.
    * the third element is what remains of `data` after parsing it according to
      `op_name`.

  """
  @spec parse_resp(atom, binary, Dict.t) ::
    :incomplete |
    {integer, {:ok, term} | {:error, term}, binary}
  def parse_resp(op_name, data, schema) do
    case parse_header(data) do
      :incomplete ->
        :incomplete
      {:ok, sid, rest} ->
        case parse_resp_contents(op_name, rest, schema) do
          :incomplete ->
            :incomplete
          {resp, rest} ->
            {sid, {:ok, resp}, rest}
        end
      {:error, sid, rest} ->
        case parse_errors(rest) do
          :incomplete ->
            :incomplete
          {error, rest} ->
            {sid, {:error, error}, rest}
        end
    end
  end

  @doc """
  Parses the response to a connection operation.

  Works very similarly to `parse_resp/3`.
  """
  @spec parse_connection_resp(binary, atom) ::
    :incomplete |
    {integer, {:ok, term} | {:error, term}, binary}
  def parse_connection_resp(data, connection_op) do
    parse_resp(connection_op, data, %{})
  end

  def parse_push_data(@push_data <> rest, schema) do
    do_parse_push_data(rest, schema)
  end

  defp parse_header(@ok <> <<sid :: int, rest :: binary>>),
    do: {:ok, sid, rest}
  defp parse_header(@error <> <<sid :: int, rest :: binary>>),
    do: {:error, sid, rest}
  defp parse_header(_),
    do: :incomplete

  defp parse_errors(data) do
    case parse_errors(data, []) do
      {errors, rest} -> {%Error{errors: errors}, rest}
      :incomplete    -> :incomplete
    end
  end

  defp parse_errors(<<1, rest :: binary>>, acc) do
    case GP.parse(rest, [&decode_term(&1, :string), &decode_term(&1, :string)]) do
      {[class, message], rest} -> parse_errors(rest, [{class, message}|acc])
      :incomplete              -> :incomplete
    end
  end

  defp parse_errors(<<0, rest :: binary>>, acc) do
    case decode_term(rest, :bytes) do
      {_exception_dump, rest} -> {Enum.reverse(acc), rest}
      :incomplete             -> :incomplete
    end
  end

  defp parse_resp_contents(:connect, data, _) do
    GP.parse(data, [&decode_term(&1, :int), &decode_term(&1, :bytes)])
  end

  defp parse_resp_contents(:db_open, data, _) do
    parsers = [
      &decode_term(&1, :int),   # sid
      &decode_term(&1, :bytes), # token
      GP.array_parser(
        &decode_term(&1, :short),                       # number of clusters
        [&decode_term(&1, :string), &decode_term(&1, :short)] # cluster name + cluster id
      ),
      &decode_term(&1, :bytes), # cluster config
      &decode_term(&1, :string), # orientdb release
    ]

    case GP.parse(data, parsers) do
      {[sid, token, _clusters, _config, _release], rest} -> {[sid, token], rest}
      :incomplete                                        -> :incomplete
    end
  end

  defp parse_resp_contents(:db_create, rest, _), do: {nil, rest}

  defp parse_resp_contents(:db_exist, data, _) do
    case decode_term(data, :byte) do
      {exists?, rest} -> {exists? == 1, rest}
      :incomplete     -> :incomplete
    end
  end

  defp parse_resp_contents(:db_drop, rest, _), do: {nil, rest}

  defp parse_resp_contents(:db_size, data, _), do: decode_term(data, :long)

  defp parse_resp_contents(:db_countrecords, data, _), do: decode_term(data, :long)

  defp parse_resp_contents(:db_reload, data, _) do
    cluster_parsers = [&decode_term(&1, :string), &decode_term(&1, :short)]
    array_parser    = GP.array_parser(&decode_term(&1, :short), cluster_parsers)
    GP.parse(data, array_parser)
  end

  # REQUEST_RECORD_LOAD and REQUEST_RECORD_LOAD_IF_VERSION_NOT_LATEST reply in
  # the exact same way.
  defp parse_resp_contents(op, data, schema)
      when op in [:record_load, :record_load_if_version_not_latest] do
    parse_resp_to_record_load(data, [], schema)
  end

  defp parse_resp_contents(:record_create, data, _) do
    parsers = [
      &decode_term(&1, :short), # cluster id
      &decode_term(&1, :long),  # cluster position
      &decode_term(&1, :int),   # version
      &parse_collection_changes/1,
    ]

    case GP.parse(data, parsers) do
      {[cluster_id, position, version, _], rest} ->
        {{%RID{cluster_id: cluster_id, position: position}, version}, rest}
      :incomplete ->
        :incomplete
    end
  end

  defp parse_resp_contents(:record_update, data, _) do
    parsers = [
      &decode_term(&1, :int), # version
      &parse_collection_changes/1,
    ]

    case GP.parse(data, parsers) do
      {[version, _], rest} -> {version, rest}
      :incomplete          -> :incomplete
    end
  end

  defp parse_resp_contents(:record_delete, data, _) do
    case decode_term(data, :byte) do
      {0, rest}   -> {false, rest}
      {1, rest}   -> {true, rest}
      :incomplete -> :incomplete
    end
  end

  defp parse_resp_contents(:command, data, schema) do
    parse_resp_to_command(data, schema)
  end

  defp parse_resp_contents(:tx_commit, data, _) do
    created_parser = GP.array_parser(&decode_term(&1, :int), [
      &decode_term(&1, :short), # client cluster id
      &decode_term(&1, :long), # client cluster position
      &decode_term(&1, :short), # cluster id
      &decode_term(&1, :long), # cluster position
    ])

    updated_parser = GP.array_parser(&decode_term(&1, :int), [
      &decode_term(&1, :short), # updated cluster id
      &decode_term(&1, :long), # updated cluster position
      &decode_term(&1, :int), # new record version
    ])

    parsers = [created_parser, updated_parser, &parse_collection_changes/1]

    case GP.parse(data, parsers) do
      :incomplete ->
        :incomplete
      {[created, updated, _coll_changes], rest} ->
        {build_resp_to_tx_commit(created, updated), rest}
    end
  end

  defp parse_resp_to_record_load(<<1, rest :: binary>>, acc, schema) do
    parsers = [
      &decode_term(&1, :byte),  # type
      &decode_term(&1, :int),   # version
      &decode_term(&1, :bytes), # contents
    ]

    case GP.parse(rest, parsers) do
      {[@document, version, content], rest} ->
        case RecordSerialization.decode(content, schema) do
          :unknown_property_id ->
            record = %UndecodedDocument{version: version, content: content}
            parse_resp_to_record_load(rest, [record|acc], schema)
          document ->
            document = %{document | version: version}
            parse_resp_to_record_load(rest, [document|acc], schema)
        end
      {[@binary_record, version, content], rest} ->
        record = %BinaryRecord{contents: content, version: version}
        parse_resp_to_record_load(rest, [record|acc], schema)
      :incomplete ->
        :incomplete
    end
  end

  defp parse_resp_to_record_load(<<0, rest :: binary>>, acc, _) do
    case Enum.reverse(acc) do
      [record|linked_records] ->
        {{record, build_linked_records(linked_records)}, rest}
      [] ->
        {{nil, build_linked_records([])}, rest}
    end
  end

  defp parse_resp_to_record_load(_, _acc, _) do
    :incomplete
  end

  @null_result       ?n
  @list              ?l
  @set               ?s
  @single_record     ?r
  @serialized_result ?a

  defp parse_resp_to_command(<<type, data :: binary>>, schema) when type in [@list, @set] do
    parser = GP.array_parser(&decode_term(&1, :int), &parse_record_with_rid(&1, schema))

    case GP.parse(data, parser) do
      {records, rest} ->
        parse_linked_records(records, rest, schema)
      _ ->
        :incomplete
    end
  end

  defp parse_resp_to_command(<<@single_record, rest :: binary>>, schema) do
    case GP.parse(rest, &parse_record_with_rid(&1, schema)) do
      {record, rest} ->
        parse_linked_records(record, rest, schema)
      :incomplete ->
        :incomplete
    end
  end

  defp parse_resp_to_command(<<@serialized_result, rest :: binary>>, schema) do
    case GP.parse(rest, &decode_term(&1, :bytes)) do
      {binary, rest} ->
        parse_linked_records(CSVTypes.decode(binary), rest, schema)
      :incomplete ->
        :incomplete
    end
  end

  defp parse_resp_to_command(<<@null_result, rest :: binary>>, schema) do
    parse_linked_records(nil, rest, schema)
  end

  defp parse_resp_to_command(_, _) do
    :incomplete
  end

  # Meaning of the first two bytes in a record definition:
  # 0  - full-fledged record
  # -2 - null record
  # -3 - RID only (cluster_id as a short, cluster_position as a long)

  defp parse_record_with_rid(<<0 :: short, rest :: binary>>, schema) do
    parsers = [
      &decode_term(&1, :byte),
      &decode_term(&1, :short),
      &decode_term(&1, :long),
      &decode_term(&1, :int),
      &decode_term(&1, :bytes)
    ]

    case GP.parse(rest, parsers) do
      {[?d, cluster_id, cluster_pos, version, record_content], rest} ->
        rid = %RID{cluster_id: cluster_id, position: cluster_pos}

        record = case RecordSerialization.decode(record_content, schema) do
          :unknown_property_id ->
            %UndecodedDocument{version: version, rid: rid, content: record_content}
          record ->
            %{record | version: version, rid: rid}
        end

        {record, rest}
      :incomplete ->
        :incomplete
    end
  end

  defp parse_record_with_rid(<<-2 :: short, rest :: binary>>, _schema) do
    {nil, rest}
  end

  defp parse_record_with_rid(<<-3 :: short, rest :: binary>>, _schema) do
    GP.parse(rest, [&decode_term(&1, :short), &decode_term(&1, :long)])
  end

  defp parse_record_with_rid(_, _schema) do
    :incomplete
  end

  defp parse_collection_changes(data) do
    array_elem_parsers = [
      &decode_term(&1, :long),
      &decode_term(&1, :long),
      &decode_term(&1, :long),
      &decode_term(&1, :long),
      &decode_term(&1, :int),
    ]

    GP.parse(data, GP.array_parser(&decode_term(&1, :int), array_elem_parsers))
  end

  defp build_resp_to_tx_commit(created, updated) do
    # This function assumes that the temporary ids assigned to the record to
    # create have been assigned sequentially: -2, -3, -4 and so on. This
    # information is used to order the created rids that occur in the response.

    created = Enum.map created, fn([-1, client_id, cluster_id, pos]) ->
      # The version is 0 by default, if it's not 0 then the record will appear
      # in the updated list too with the right version. The rest of this
      # function will update the version.
      {client_id, {%RID{cluster_id: cluster_id, position: pos}, 0}}
    end

    acc = {[], created}
    {updated, created} = Enum.reduce updated, acc, fn([cid, pos, vsn], {updated, created}) ->
      rid = %RID{cluster_id: cid, position: pos}

      if index = Enum.find_index(created, &match?({_, {^rid, 0}}, &1)) do
        new_created = List.update_at(created, index, fn({client_id, _}) -> {client_id, {rid, vsn}} end)
        {updated, new_created}
      else
        {[{rid, vsn}|updated], created}
      end
    end

    created =
      created
      |> Enum.sort_by(fn({client_id, _}) -> - client_id end)
      |> Enum.map(fn({_, rid_and_vsn}) -> rid_and_vsn end)

    %{created: created, updated: Enum.reverse(updated)}
  end

  defp parse_linked_records(existing_records, data, schema) do
    case parse_only_linked_records(data, schema, []) do
      {records, rest} ->
        resp = %{
          response: existing_records,
          linked_records: build_linked_records(records)
        }
        {resp, rest}
      :incomplete ->
        :incomplete
    end
  end

  defp parse_only_linked_records(<<0, rest :: binary>>, _schema, acc) do
    {Enum.reverse(acc), rest}
  end

  defp parse_only_linked_records(<<2, rest :: binary>>, schema, acc) do
    case parse_record_with_rid(rest, schema) do
      {record, rest} ->
        parse_only_linked_records(rest, schema, [record|acc])
      :incomplete ->
        :incomplete
    end
  end

  defp parse_only_linked_records(_, _schema, _acc) do
    :incomplete
  end

  defp build_linked_records(records) do
    for %{rid: rid} = record <- records, into: HashDict.new do
      {rid, record}
    end
  end

  defp do_parse_push_data(data, schema) do
    parsers = [
      &decode_term(&1, :int), # in Java this is Integer.MIN_VALUE, wat
      &decode_term(&1, :byte), # the byte for REQUEST_PUSH_LIVE_QUERY
      &decode_term(&1, :bytes), # the list of changes
    ]

    case GP.parse(data, parsers) do
      :incomplete ->
        :incomplete
      {[_int_min_value, 81, content], rest} ->
        {:ok, parse_live_query_content(content, schema), rest}
    end
  end

  defp parse_live_query_content(<<op_type, token :: int, record_type, version :: int, cluster_id :: short, position :: long, rest :: binary>>, schema) do
    {record_content, <<>>} = decode_term(rest, :bytes)

    rid = %RID{cluster_id: cluster_id, position: position}

    record = decode_record_by_type(record_type, record_content, schema)
    record = %{record | rid: rid, version: version}

    op =
      case op_type do
        1 -> :update
        2 -> :delete
        3 -> :create
      end

    resp = {op, record}

    {token, resp}
  end

  defp decode_record_by_type(@document, content, schema) do
    RecordSerialization.decode(content, schema)
  end

  defp decode_record_by_type(@binary_record, content, _schema) do
    %BinaryRecord{contents: content}
  end

  defp req_code(:shutdown),                          do: 1
  defp req_code(:connect),                           do: 2
  defp req_code(:db_open),                           do: 3
  defp req_code(:db_create),                         do: 4
  defp req_code(:db_exist),                          do: 6
  defp req_code(:db_drop),                           do: 7
  defp req_code(:config_get),                        do: 70
  defp req_code(:config_set),                        do: 71
  defp req_code(:config_list),                       do: 72
  defp req_code(:db_list),                           do: 74
  defp req_code(:db_close),                          do: 5
  defp req_code(:db_size),                           do: 8
  defp req_code(:db_countrecords),                   do: 9
  defp req_code(:record_load),                       do: 30
  defp req_code(:record_load_if_version_not_latest), do: 44
  defp req_code(:record_create),                     do: 31
  defp req_code(:record_update),                     do: 32
  defp req_code(:record_delete),                     do: 33
  defp req_code(:record_copy),                       do: 34
  defp req_code(:positions_floor),                   do: 39
  defp req_code(:command),                           do: 41
  defp req_code(:positions_ceiling),                 do: 42
  defp req_code(:tx_commit),                         do: 60
  defp req_code(:index_get),                         do: 60
  defp req_code(:index_put),                         do: 60
  defp req_code(:index_remove),                      do: 60
  defp req_code(:db_reload),                         do: 73
  defp req_code(:push_record),                       do: 79
  defp req_code(:push_distrib_config),               do: 80
  defp req_code(:replication),                       do: 91
  defp req_code(:db_transfer),                       do: 93
  defp req_code(:db_freeze),                         do: 94
  defp req_code(:db_release),                        do: 95
  defp req_code(:create_sbtree_bonsai),              do: 110
  defp req_code(:sbtree_bonsai_get),                 do: 111
  defp req_code(:sbtree_bonsai_first_key),           do: 112
  defp req_code(:sbtree_bonsai_get_entries_major),   do: 113
  defp req_code(:ridbag_get_size),                   do: 114
end
