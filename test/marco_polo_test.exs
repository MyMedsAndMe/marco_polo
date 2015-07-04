defmodule MarcoPoloTest do
  use ExUnit.Case, async: true

  @db_name TestHelpers.db_name()

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
    assert {:ok, true}  = MarcoPolo.db_exists?(c, TestHelpers.db_name(), "plocal")
    assert {:ok, false} = MarcoPolo.db_exists?(c, "nonexistent", "plocal")
  end

  test "create_db/4" do
    {:ok, c} = conn_server()
    assert :ok = MarcoPolo.create_db(c, "MarcoPoloTestGeneratedDb", :document, :plocal)
  end

  test "drop_db/3" do
    {:ok, c} = conn_server()
    assert :ok = MarcoPolo.drop_db(c, "MarcoPoloToDrop", :memory)
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

  test "load_record/4" do
    {:ok, c} = conn_db()
    rid      = TestHelpers.record_rid("record_load")

    {:ok, [record]} = MarcoPolo.load_record(c, rid, "*:-1")

    assert %MarcoPolo.Record{} = record
    assert record.version == 1
    assert record.class   == "Schemaless"
    assert record.fields  == %{"name" => "record_load"}
  end

  test "load_record/4 using the :if_version_not_latest option" do
    {:ok, c} = conn_db()
    rid      = TestHelpers.record_rid("record_load")

    assert {:ok, []} = MarcoPolo.load_record(c, rid, "*:-1", version: 1, if_version_not_latest: true)
  end

  test "delete_record/3" do
    {:ok, c} = conn_db()
    version  = 1
    rid      = TestHelpers.record_rid("record_delete")

    # Wrong version causes no deletions.
    assert {:ok, false} = MarcoPolo.delete_record(c, rid, version + 100)

    assert {:ok, true}  = MarcoPolo.delete_record(c, rid, version)
    assert {:ok, false} = MarcoPolo.delete_record(c, rid, version)
  end

  test "create_record/3" do
    {:ok, c} = conn_db()
    cluster_id = TestHelpers.cluster_id()
    record = %MarcoPolo.Record{class: "Schemaless", fields: %{"foo" => "bar"}}

    {:ok, {rid, version}} = MarcoPolo.create_record(c, cluster_id, record)

    assert %MarcoPolo.RID{cluster_id: ^cluster_id} = rid
    assert is_integer(version)
  end

  test "command/3: SELECT query" do
    {:ok, c}       = conn_db()
    {:ok, records} = MarcoPolo.command(c, "SELECT FROM Schemaless", fetch_plan: "*:-1")

    assert Enum.find(records, fn record ->
      assert %MarcoPolo.Record{} = record
      assert record.class == "Schemaless"

      record.fields["name"] == "record_load"
    end)
  end

  defp conn_server do
    MarcoPolo.start_link(connection: :server,
                         user: TestHelpers.user(),
                         password: TestHelpers.password())
  end

  defp conn_db do
    MarcoPolo.start_link(connection: {:db, TestHelpers.db_name(), "plocal"},
                         user: TestHelpers.user(),
                         password: TestHelpers.password())
  end
end
