defmodule MarcoPolo.Protocol do
  @moduledoc false

  require Integer

  import MarcoPolo.Protocol.BinaryHelpers

  alias MarcoPolo.GenericParser, as: GP
  alias MarcoPolo.Error
  alias MarcoPolo.Protocol.RecordSerialization

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

  @type sid :: non_neg_integer
  @type op_code :: non_neg_integer
  @type op_name :: atom

  @ok    <<0>>
  @error <<1>>

  def encode_op(op_name, args) do
    [req_code(op_name)|Enum.map(args, &encode_term/1)]
  end

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

  # An entire record.
  def encode_term({:record, record}), do: encode_term(RecordSerialization.encode(record))

  def parse_connection_resp(data, connection_op) do
    parse_resp(connection_op, data)
  end

  def parse_resp(op_name, data) do
    case parse_header(data) do
      :incomplete ->
        :incomplete
      {:ok, sid, rest} ->
        case parse_resp_contents(op_name, rest) do
          {resp, rest} ->
            {:ok, sid, resp, rest}
          :incomplete ->
            :incomplete
        end
      {:error, rest} ->
        case parse_errors(rest, []) do
          {errors, rest} ->
            {:error, Error.from_errors(errors), rest}
          :incomplete ->
            :incomplete
        end
    end
  end

  def parse_header(@ok <> <<sid :: int, rest :: binary>>),
    do: {:ok, sid, rest}
  def parse_header(@error <> <<_sid :: int, rest :: binary>>),
    do: {:error, rest}
  def parse_header(_),
    do: :incomplete

  def parse(<<-1 :: int, rest :: binary>>, type) when type in [:string, :bytes] do
    {nil, rest}
  end

  def parse(<<length :: int, data :: binary>>, type) when type in [:string, :bytes] do
    case data do
      <<parsed :: bytes-size(length), rest :: binary>> -> {parsed, rest}
      _                                                -> :incomplete
    end
  end

  def parse(<<byte, rest :: binary>>, :byte), do: {byte, rest}

  def parse(<<i :: short, rest :: binary>>, :short), do: {i, rest}
  def parse(<<i :: int, rest :: binary>>, :int),     do: {i, rest}
  def parse(<<i :: long, rest :: binary>>, :long),   do: {i, rest}

  def parse(_data, _type) do
    :incomplete
  end

  defp parse_errors(<<1, rest :: binary>>, acc) do
    case GP.parse(rest, [&parse(&1, :string), &parse(&1, :string)]) do
      {[class, message], rest} -> parse_errors(rest, [{class, message}|acc])
      :incomplete              -> :incomplete
    end
  end

  defp parse_errors(<<0, rest :: binary>>, acc) do
    # What am I supposed to do with a Java binary dump of the exception?! :(
    case parse(rest, :bytes) do
      {_dump, rest} -> {Enum.reverse(acc), rest}
      :incomplete   -> :incomplete
    end
  end

  defp parse_resp_contents(:connect, data) do
    GP.parse(data, [&parse(&1, :int), &parse(&1, :bytes)])
  end

  defp parse_resp_contents(:db_open, data) do
    parsers = [
      &parse(&1, :int),   # sid
      &parse(&1, :bytes), # token
      GP.array_parser(
        &parse(&1, :short),                       # number of clusters
        [&parse(&1, :string), &parse(&1, :short)] # cluster name + cluster id
      ),
      &parse(&1, :bytes), # cluster config
      &parse(&1, :string), # orientdb release
    ]

    case GP.parse(data, parsers) do
      {[sid, token, _clusters, _config, _release], rest} -> {[sid, token], rest}
      :incomplete                                        -> :incomplete
    end
  end

  defp parse_resp_contents(:db_create, rest), do: {nil, rest}

  defp parse_resp_contents(:db_exist, data) do
    case parse(data, :byte) do
      {exists?, rest} -> {exists? == 1, rest}
      :incomplete     -> :incomplete
    end
  end

  defp parse_resp_contents(:db_drop, rest), do: {nil, rest}

  defp parse_resp_contents(:db_size, data), do: parse(data, :long)

  defp parse_resp_contents(:db_countrecords, data), do: parse(data, :long)

  defp parse_resp_contents(:db_reload, data) do
    cluster_parsers = [&parse(&1, :string), &parse(&1, :short)]
    array_parser    = GP.array_parser(&parse(&1, :short), cluster_parsers)
    GP.parse(data, array_parser)
  end

  # REQUEST_RECORD_LOAD and REQUEST_RECORD_LOAD_IF_VERSION_NOT_LATEST reply in
  # the exact same way.
  defp parse_resp_contents(op, data) when op in [:record_load, :record_load_if_version_not_latest] do
    data |> parse_resp_to_record_load([]) |> Enum.reverse
  end

  defp parse_resp_contents(:record_delete, data) do
    case parse(data, :byte) do
      {0, rest}   -> {false, rest}
      {1, rest}   -> {true, rest}
      :incomplete -> :incomplete
    end
  end

  @null_result       ?n
  @list              ?l
  @set               ?s
  @single_record     ?r
  @serialized_result ?a

  defp parse_resp_contents(:command, data) do
    parse_resp_to_command(data)
  end

  defp parse_resp_to_record_load(<<1, rest :: binary>>, acc) do
    parsers = [
      &parse(&1, :byte),  # version
      &parse(&1, :int),   # type
      &parse(&1, :bytes), # contents
    ]

    case GP.parse(rest, parsers) do
      {[type, version, record_content], rest} ->
        {class_name, fields} = RecordSerialization.decode(record_content)
        record = %MarcoPolo.Record{class: class_name, fields: fields, version: version}
        parse_resp_to_record_load(rest, [{record_type(type), record}|acc])
      :incomplete ->
        :incomplete
    end
  end

  defp parse_resp_to_record_load(<<0>>, acc) do
    acc
  end

  defp parse_resp_to_record_load(_, _acc) do
    :incomplete
  end

  defp parse_resp_to_command(<<type, data :: binary>>) when type in [@list, @set] do
    parsers = [GP.array_parser(&parse(&1, :int), &parse_record_with_rid/1), &parse(&1, :byte)]

    case GP.parse(data, parsers) do
      # TODO find out why OrientDB shoves a 0 byte at the end of this list, not
      # mentioned in the docs :(
      {[records, 0], rest} -> {records, rest}
      _                    -> :incomplete
    end
  end

  defp parse_resp_to_command(<<@single_record, rest :: binary>>) do
    {record, rest} = parse_record_with_rid(rest)

    # TODO find out why OrientDB shoves a 0 byte at the end of this list, not
    # mentioned in the docs :(
    <<0>> = rest

    record
  end

  defp parse_resp_to_command(_) do
    :incomplete
  end

  # Meaning of the first two bytes in a record definition:
  # 0  - full-fledged record
  # -2 - null record
  # -3 - RID only (cluster_id as a short, cluster_position as a long)

  defp parse_record_with_rid(<<0 :: short, rest :: binary>>) do
    parsers = [
      &parse(&1, :byte),
      &parse(&1, :short),
      &parse(&1, :long),
      &parse(&1, :int),
      &parse(&1, :bytes)
    ]

    case GP.parse(rest, parsers) do
      {[record_type, _cluster_id, _cluster_pos, version, record_content], rest} ->
        {class_name, fields} = RecordSerialization.decode(record_content)
        record = %MarcoPolo.Record{class: class_name, fields: fields, version: version}
        {{record_type(record_type), record}, rest}
      :incomplete ->
        :incomplete
    end
  end

  defp parse_record_with_rid(<<-2 :: short, rest :: binary>>) do
    {nil, rest}
  end

  defp parse_record_with_rid(<<-3 :: short, rest :: binary>>) do
    GP.parse(rest, [&parse(&1, :short), &parse(&1, :long)])
  end

  defp parse_record_with_rid(_) do
    :incomplete
  end

  defp record_type(?d), do: :document

  defp req_code(:shutdown),                        do: 1
  defp req_code(:connect),                         do: 2
  defp req_code(:db_open),                         do: 3
  defp req_code(:db_create),                       do: 4
  defp req_code(:db_exist),                        do: 6
  defp req_code(:db_drop),                         do: 7
  defp req_code(:config_get),                      do: 70
  defp req_code(:config_set),                      do: 71
  defp req_code(:config_list),                     do: 72
  defp req_code(:db_list),                         do: 74
  defp req_code(:db_close),                        do: 5
  defp req_code(:db_size),                         do: 8
  defp req_code(:db_countrecords),                 do: 9
  defp req_code(:datacluster_copy),                do: 14
  defp req_code(:datacluster_lh_cluster_is_used),  do: 16
  defp req_code(:record_metadata),                 do: 29
  defp req_code(:record_load),                     do: 30
  defp req_code(:record_create),                   do: 31
  defp req_code(:record_update),                   do: 32
  defp req_code(:record_delete),                   do: 33
  defp req_code(:record_copy),                     do: 34
  defp req_code(:record_clean_out),                do: 38
  defp req_code(:positions_floor),                 do: 39
  defp req_code(:command),                         do: 41
  defp req_code(:positions_ceiling),               do: 42
  defp req_code(:tx_commit),                       do: 60
  defp req_code(:db_reload),                       do: 73
  defp req_code(:push_record),                     do: 79
  defp req_code(:push_distrib_config),             do: 80
  defp req_code(:db_copy),                         do: 90
  defp req_code(:replication),                     do: 91
  defp req_code(:cluster),                         do: 92
  defp req_code(:db_transfer),                     do: 93
  defp req_code(:db_freeze),                       do: 94
  defp req_code(:db_release),                      do: 95
  defp req_code(:create_sbtree_bonsai),            do: 110
  defp req_code(:sbtree_bonsai_get),               do: 111
  defp req_code(:sbtree_bonsai_first_key),         do: 112
  defp req_code(:sbtree_bonsai_get_entries_major), do: 113
  defp req_code(:ridbag_get_size),                 do: 114
end
