defmodule MarcoPolo.Binary do
  import MarcoPolo.Binary.Helpers
  import MarcoPolo.Binary.Decoder
  alias MarcoPolo.Binary.Encoder

  @ok 0
  @error 1

  @empty Encoder.encode(<<>>)

  @spec encode_req(atom, integer, term) :: iodata
  def encode_req(operation, sid, args) do
    [req_code(operation),
     Encoder.encode(sid),
     Enum.map(args, &Encoder.encode/1)]
  end

  def decode_resp(operation, <<@error, sid :: int, rest :: binary>>) do
    {:error, sid, decode_error(rest)}
  end

  # CONNECT and DB_OPEN are special request with a special response which
  # doesn't include a token (since they both return the initial token); for this
  # reason, we're treating them differently.

  def decode_resp(:connect, <<@ok, sid :: int, rest :: binary>>) do
    {[new_sid, new_token], <<>>} = decode_multiple(rest, [:int, :bytes])
    {:ok, sid, :connect, [new_sid, new_token]}
  end

  def decode_resp(:db_open, <<@ok, sid :: int, rest :: binary>>) do
    {[new_sid, new_token], rest} = decode_multiple(rest, [:int, :bytes])
    {num_of_clusters, rest}      = decode(rest, :short)
    {clusters, rest}             = decode_array(rest, num_of_clusters, [:string, :short])
    {[cluster_config, orientdb_release], <<>>} = decode_multiple(rest, [:bytes, :string])
    {:ok, sid, :db_open, [new_sid, new_token, num_of_clusters, clusters, cluster_config, orientdb_release]}
  end

  def decode_resp(operation, <<@ok, sid :: int, rest :: binary>>) do
    {token, contents} = decode(rest, :bytes)
    {:ok, sid, token, decode_resp_for_req(operation, rest)}
  end

  defp decode_error(content) do
    :error
  end

  # Custom decoding of responses based on the request operation.

  defp decode_resp_for_req(:shutdown, @empty) do
    nil
  end

  defp decode_resp_for_req(:connect, <<new_sid :: int, rest :: binary>>) do
    {token, <<>>} = decode(rest, :bytes)
    {new_sid, token}
  end

  defp decode_resp_for_req(:db_open, data) do
    data
    # {wat, rest} = decode(data, :string)
    # {num_of_clusters, rest} = decode(data, :short)
    # {clusters, rest} = decode_array(rest, num_of_clusters, [:string, :short])
    # {orientdb_release, rest} = decode(rest, :string)
    # [clusters, orientdb_release]
  end

  defp decode_resp_for_req(:db_create, @empty) do
    nil
  end

  # TODO: no response, the socket is just closed at server side
  defp decode_resp_for_req(:db_close, <<>>) do
    nil
  end

  defp decode_resp_for_req(:db_exist, data) do
    {bool, <<>>} = decode(data, :boolean)
    bool
  end

  defp decode_resp_for_req(:db_reload, <<num_of_clusters :: short, rest :: binary>>) do
    {clusters, <<>>} = decode_array(rest, num_of_clusters, [:string, :short])
    clusters
  end

  defp decode_resp_for_req(:db_drop, @empty) do
    nil
  end

  defp decode_resp_for_req(:db_size, <<size :: long>>) do
    size
  end

  defp decode_resp_for_req(:db_countrecords, <<count :: long>>) do
    count
  end

  defp decode_resp_for_req(:record_load, data) do
    # [(payload-status:byte)[(record-type:byte)(record-version:int)(record-content:bytes)]*]+
  end

  defp decode_resp_for_req(:record_create, data) do
    # (cluster-id:short)(cluster-position:long)(record-version:int)(count-of-collection-changes)[(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]*
  end

  defp decode_resp_for_req(:record_update, data) do
  # (record-version:int)(count-of-collection-changes)[(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]*
  end

  defp decode_resp_for_req(:record_delete, data) do
    {deleted?, <<>>} = decode(data, :boolean)
    deleted?
  end

  defp decode_resp_for_req(:command, data) do
    # TODO
  end

  defp decode_resp_for_req(:tx_commit, data) do
    # TODO
  end

  defp decode_resp_for_req(:create_sbtree_bonsai, data) do
    # TODO
  end

  defp decode_resp_for_req(:sbtree_bonsai_get, data) do
    # TODO
  end

  defp decode_resp_for_req(:sbtree_bonsai_first_key, data) do
    # TODO
  end

  defp decode_resp_for_req(:sbtree_bonsai_get_entries_major, data) do
    # TODO
  end

  defp decode_resp_for_req(:ridbag_get_size, <<size :: int>>) do
    size
  end

  defp req_code(operation)

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
