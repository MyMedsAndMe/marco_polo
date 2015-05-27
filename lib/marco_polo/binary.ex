defmodule MarcoPolo.Binary do
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
