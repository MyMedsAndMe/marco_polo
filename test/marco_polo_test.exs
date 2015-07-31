defmodule MarcoPoloTest do
  use ExUnit.Case, async: true
  @moduletag :integration

  alias MarcoPolo.Error
  alias MarcoPolo.RID
  alias MarcoPolo.Document
  alias MarcoPolo.BinaryRecord

  test "start_link/1: raises if no connection type is specified" do
    Logger.remove_backend(:console, flush: true)
    Process.flag :trap_exit, true

    {:ok, pid} = MarcoPolo.start_link user: "foo", password: "foo", debug: {:log_to_file, "/dev/null"}

    assert_receive {:EXIT, ^pid, {error, _}}
    assert Exception.message(error) =~ "key :connection not found"

    Logger.add_backend(:console, flush: true)
  end

  test "start_link/1: raises if the connection type is unknown" do
    Process.flag :trap_exit, true
    Logger.remove_backend(:console, flush: true)
    {:ok, pid} = MarcoPolo.start_link(user: "foo", password: "foo", connection: :foo)

    assert_receive {:EXIT, ^pid, {error, _}}

    msg = "invalid connection type, valid ones are :server or {:db, name, type}"
    assert Exception.message(error) == msg

    Logger.add_backend(:console, flush: true)
  end

  defmodule ConnectedToServer do
    use ExUnit.Case, async: true
    @moduletag :integration

    setup do
      {:ok, conn} = MarcoPolo.start_link(
        connection: :server,
        user: TestHelpers.user(),
        password: TestHelpers.password()
      )

      on_exit fn -> MarcoPolo.stop(conn) end

      {:ok, %{conn: conn}}
    end

    test "db_exists?/3", %{conn: c}  do
      assert {:ok, true}  = MarcoPolo.db_exists?(c, "MarcoPoloTest", :plocal)
      assert {:ok, false} = MarcoPolo.db_exists?(c, "nonexistent", :plocal)
    end

    test "create_db/4 with a database that doens't exist yet", %{conn: c} do
      assert :ok = MarcoPolo.create_db(c, "MarcoPoloTestGenerated", :document, :plocal)
    end

    test "create_db/4 with a database that already exists", %{conn: c} do
      assert {:error, %Error{} = err} = MarcoPolo.create_db(c, "MarcoPoloTest", :document, :plocal)
      assert [{exception, msg}] = err.errors
      assert exception == "com.orientechnologies.orient.core.exception.ODatabaseException"
      assert msg       =~ "Database named 'MarcoPoloTest' already exists:"
    end

    test "drop_db/3 with an existing database", %{conn: c} do
      assert :ok = MarcoPolo.drop_db(c, "MarcoPoloToDrop", :memory)
    end

    test "drop_db/3 with a non-existing database", %{conn: c} do
      expected = {"com.orientechnologies.orient.core.exception.OStorageException",
                  "Database with name 'Nonexistent' doesn't exits."}

      assert {:error, %MarcoPolo.Error{} = err} = MarcoPolo.drop_db(c, "Nonexistent", :plocal)
      assert hd(err.errors) == expected
    end
  end

  defmodule ConnectedToDb do
    use ExUnit.Case, async: true
    @moduletag :integration

    setup do
      {:ok, conn} = MarcoPolo.start_link(
        connection: {:db, "MarcoPoloTest", :document},
        user: TestHelpers.user(),
        password: TestHelpers.password()
      )

      on_exit fn -> MarcoPolo.stop(conn) end

      {:ok, %{conn: conn}}
    end

    test "db_reload/1", %{conn: c} do
      assert :ok = MarcoPolo.db_reload(c)
    end

    test "db_size/1", %{conn: c} do
      assert {:ok, size} = MarcoPolo.db_size(c)
      assert is_integer(size)
    end

    test "db_countrecords/1", %{conn: c} do
      assert {:ok, nrecords} = MarcoPolo.db_countrecords(c)
      assert is_integer(nrecords)
    end

    test "load_record/4: loading a document", %{conn: c} do
      rid = TestHelpers.record_rid("record_load")

      {:ok, [record]} = MarcoPolo.load_record(c, rid, fetch_plan: "*:-1")

      assert %Document{} = record
      assert record.version == 1
      assert record.class   == "Schemaless"
      assert record.fields  == %{"name" => "record_load", "f" => "foo"}

      rid = TestHelpers.record_rid("schemaless_record_load")

      {:ok, [record]} = MarcoPolo.load_record(c, rid)
      assert record.version == 1
      assert record.class == "Schemaful"
      assert record.fields == %{"myString" => "record_load"}
    end

    test "load_record/4 using the :if_version_not_latest option", %{conn: c} do
      rid = TestHelpers.record_rid("record_load")

      assert {:ok, []} = MarcoPolo.load_record(c, rid, version: 1, if_version_not_latest: true)
    end

    test "delete_record/3", %{conn: c} do
      version  = 1
      rid      = TestHelpers.record_rid("record_delete")

      # Wrong version causes no deletions.
      assert {:ok, false} = MarcoPolo.delete_record(c, rid, version + 100)

      assert {:ok, true}  = MarcoPolo.delete_record(c, rid, version)
      assert {:ok, false} = MarcoPolo.delete_record(c, rid, version)
    end

    test "create_record/3: creating a record (document) synchronously", %{conn: c} do
      cluster_id = TestHelpers.cluster_id("schemaless")
      record = %Document{class: "Schemaless", fields: %{"foo" => "bar"}}

      {:ok, {rid, version}} = MarcoPolo.create_record(c, cluster_id, record)

      assert %MarcoPolo.RID{cluster_id: ^cluster_id} = rid
      assert is_integer(version)
    end

    test "create_record/3: creating a record (blob) synchronously", %{conn: c} do
      cluster_id = TestHelpers.cluster_id("schemaless")
      record = %BinaryRecord{contents: <<84, 41>>}

      {:ok, {rid, version}} = MarcoPolo.create_record(c, cluster_id, record)

      assert %MarcoPolo.RID{cluster_id: ^cluster_id} = rid
      assert is_integer(version)
    end

    test "create_record/3: using the :no_response option", %{conn: c} do
      cluster_id = TestHelpers.cluster_id("schemaless")
      record = %Document{class: "Schemaless", fields: %{"foo" => "bar"}}

      :ok = MarcoPolo.create_record(c, cluster_id, record, no_response: true)
    end

    test "update_record/6 synchronously", %{conn: c} do
      rid = TestHelpers.record_rid("record_update")
      new_doc = %Document{class: "Schemaless", fields: %{f: "bar"}}

      assert {:ok, new_version} = MarcoPolo.update_record(c, rid, 1, new_doc, true)
      assert is_integer(new_version)
    end

    test "update_record/6 asynchronously (:no_response option)", %{conn: c} do
      rid = TestHelpers.record_rid("record_update")
      new_doc = %Document{class: "Schemaless", fields: %{f: "baz"}}

      assert :ok = MarcoPolo.update_record(c, rid, 1, new_doc, true, no_response: true)
    end

    test "command/3: SELECT query without a WHERE clause", %{conn: c} do
      {:ok, %{response: records}} = MarcoPolo.command(c, "SELECT FROM Schemaless", fetch_plan: "*:0")

      assert Enum.find(records, fn record ->
        assert %Document{} = record
        assert record.class == "Schemaless"

        record.fields["name"] == "record_load"
      end)
    end

    test "command/3: SELECT query with a WHERE clause", %{conn: c} do
      cmd = "SELECT FROM Schemaless WHERE name = 'record_load' LIMIT 1"
      res = MarcoPolo.command(c, cmd, fetch_plan: "*:-1")

      assert {:ok, %{response: [%Document{} = record]}} = res
      assert record.fields["name"] == "record_load"
    end

    test "command/3: SELECT query with named parameters", %{conn: c} do
      cmd    = "SELECT FROM Schemaless WHERE name = :name"
      params = %{"name" => "record_load"}
      res    = MarcoPolo.command(c, cmd, fetch_plan: "*:-1", params: params)

      assert {:ok, %{response: [%Document{} = record]}} = res
      assert record.fields["name"] == "record_load"
    end

    test "command/3: SELECT query with positional parameters", %{conn: c} do
      cmd = "SELECT FROM Schemaless WHERE name = ? AND f = ?"
      params = ["record_load", "foo"]
      res = MarcoPolo.command(c, cmd, params: params)

      assert {:ok, %{response: [%Document{} = doc]}} = res
      assert doc.fields["name"] == "record_load"
      assert doc.fields["f"] == "foo"
    end

    test "command/3: INSERT query inserting multiple records", %{conn: c} do
      cmd = "INSERT INTO Schemaless(my_field) VALUES ('value1'), ('value2')"

      assert {:ok, %{response: [r1, r2]}} = MarcoPolo.command(c, cmd)
      assert r1.fields["my_field"] == "value1"
      assert r2.fields["my_field"] == "value2"
    end

    test "command/3: miscellaneous commands", %{conn: c} do
      import MarcoPolo, only: [command: 2, command: 3]

      assert {:ok, %{}} = command(c, "CREATE CLUSTER misc_tests")
      assert {:ok, %{}} = command(c, "CREATE CLASS MiscTests CLUSTER misc_tests")
      assert {:ok, %{}} = command(c, "CREATE PROPERTY MiscTests.foo DATETIME")
      assert {:ok, %{response: nil}} = command(c, "DROP PROPERTY MiscTests.foo")
      assert {:ok, %{response: true}} = command(c, "DROP CLASS MiscTests")
      assert {:ok, %{response: true}} = command(c, "DROP CLUSTER misc_tests")
      assert {:ok, %{response: false}} = command(c, "DROP CLUSTER misc_tests")
    end

    test "transaction/3: creating records", %{conn: c} do
      cluster_id = TestHelpers.cluster_id("schemaless")

      operations = [
        {:create, %Document{class: "Schemaless", fields: %{"name" => "inside_transaction"}}},
        {:create, %BinaryRecord{contents: <<1, 2, 3>>}},
      ]

      assert {:ok, %{created: created, updated: []}} = MarcoPolo.transaction(c, operations)

      assert [{%RID{cluster_id: ^cluster_id}, v1}, {%RID{}, v2}] = created
      assert is_integer(v1)
      assert is_integer(v2)
    end

    @tag :scripting
    test "script/4", %{conn: c} do
      script = """
      db.command('CREATE CLASS ScriptTest');

      for (i = 1; i <= 3; i++) {
        db.command('INSERT INTO ScriptTest(foo) VALUES("test' + i + '")');
      }
      """

      assert {:ok, _} = MarcoPolo.script(c, "Javascript", script)

      {:ok, %{response: records}} = MarcoPolo.command(c, "SELECT FROM ScriptTest", fetch_plan: "")
      records = Enum.map(records, fn(%Document{fields: %{"foo" => value}}) -> value end)

      assert records == ~w(test1 test2 test3)
    end
  end
end
