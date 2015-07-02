defmodule MarcoPoloTest do
  use ExUnit.Case, async: true

  test "start_link/1: not specifying a connection type raises an error" do
    msg = "no connection type (connect/db_open) specified"
    assert_raise ArgumentError, msg, fn ->
      MarcoPolo.start_link
    end
  end

  test "start_link/1: always returns {:ok, pid}" do
    assert {:ok, pid} = MarcoPolo.start_link(connection: :server)
    assert is_pid(pid)
  end

  test "db_exists?/3" do
    {:ok, c} = conn_server()
    assert {:ok, true}  = MarcoPolo.db_exists?(c, "GratefulDeadConcerts", "plocal")
    assert {:ok, false} = MarcoPolo.db_exists?(c, "nonexistent", "plocal")
  end

  test "db_reload/1" do
    {:ok, c} = conn_db()
    assert :ok = MarcoPolo.db_reload(c)
  end

  test "db_size/1" do
    {:ok, c} = conn_db()
    assert {:ok, size} = MarcoPolo.db_size(c)
    assert is_integer(size)
  end

  test "db_countrecords/1" do
    {:ok, c} = conn_db()
    assert {:ok, nrecords} = MarcoPolo.db_countrecords(c)
    assert is_integer(nrecords)
  end

  test "create_record/3, load_record/4 and delete_record/1" do
    {:ok, c} = conn_db()
    cluster_id = 13
    record = %MarcoPolo.Record{class: "Propertyless", fields: %{"foo" => "bar"}}

    {:ok, {rid, version}} = MarcoPolo.create_record(c, cluster_id, record)

    assert %MarcoPolo.RID{cluster_id: ^cluster_id} = rid
    assert is_integer(version)

    {:ok, [record]} = MarcoPolo.load_record(c, rid, "*:-1")

    assert %MarcoPolo.Record{} = record
    assert record.version == version
    assert record.class == "Propertyless"
    assert record.fields == %{"foo" => "bar"}

    # Wrong version doesn't delete anything.
    assert {:ok, false} = MarcoPolo.delete_record(c, rid, version + 1)

    assert {:ok, true}  = MarcoPolo.delete_record(c, rid, version)
    assert {:ok, false} = MarcoPolo.delete_record(c, rid, version)
  end

  defp conn_server do
    MarcoPolo.start_link(connection: :server, user: user(), password: pass())
  end

  defp conn_db do
    MarcoPolo.start_link(connection: {:db, "GratefulDeadConcerts", "plocal"},
                         user: user(),
                         password: pass())
  end

  defp user, do: System.get_env("ORIENTDB_USER")
  defp pass, do: System.get_env("ORIENTDB_PASS")
end
