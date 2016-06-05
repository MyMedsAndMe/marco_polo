defmodule MarcoPoloTest do
  use ExUnit.Case
  @moduletag :integration

  import MarcoPolo

  alias MarcoPolo.Error
  alias MarcoPolo.RID
  alias MarcoPolo.Document
  alias MarcoPolo.BinaryRecord
  alias MarcoPolo.FetchPlan

  setup context do
    auth_opts = [user: TestHelpers.user(), password: TestHelpers.password()]
    cond do
      context[:no_started_connection] ->
        {:ok, %{}}
      context[:connected_to_server] ->
        {:ok, conn} = start_link(auth_opts ++ [connection: :server])
        on_exit fn -> stop_conn_blocking(conn) end
        {:ok, %{conn: conn}}
      true ->
        # In this case, we'll just connect to a database.
        {:ok, conn} = start_link(auth_opts ++ [connection: {:db, "MarcoPoloTest"}])
        on_exit fn -> stop_conn_blocking(conn) end
        {:ok, %{conn: conn}}
    end
  end

  @tag :no_started_connection
  test "start_link/1: raises if no connection type is specified" do
    silence_log fn ->
      Process.flag(:trap_exit, true)
      {:ok, conn} = start_link(user: "foo", password: "foo")

      assert_receive {:EXIT, ^conn, {error, _}}
      assert Exception.message(error) =~ "key :connection not found"

      stop_conn_blocking(conn)
    end
  end

  @tag :no_started_connection
  test "start_link/1: raises if the connection type is unknown" do
    silence_log fn ->
      Process.flag(:trap_exit, true)
      {:ok, conn} = start_link(user: "foo", password: "foo", connection: :foo)

      assert_receive {:EXIT, ^conn, {error, _}}
      msg = "invalid connection type, valid ones are :server or {:db, name}"
      assert Exception.message(error) == msg

      stop_conn_blocking(conn)
    end
  end

  @tag :no_started_connection
  test "start_link/1: raises if db type is used when connecting" do
    silence_log fn ->
      Process.flag :trap_exit, true

      {:ok, conn} =
        start_link(user: "foo", password: "foo", connection: {:db, "foo", :document})

      assert_receive {:EXIT, ^conn, {error, _stacktrace}}
      msg =
        "the database type is not supported (anymore) when connecting" <>
        " to a database, use {:db, db_name} instead"
      assert Exception.message(error) == msg

      stop_conn_blocking(conn)
    end
  end

  @tag :no_started_connection
  test "stop/1" do
    {:ok, conn} = start_link(
      user: TestHelpers.user,
      password: TestHelpers.password,
      connection: :server
    )

    assert Process.alive?(conn)
    stop(conn)
    assert_pid_will_die(conn)
  end

  @tag :connected_to_server
  test "db_exists?/3", %{conn: c}  do
    assert {:ok, true}  = db_exists?(c, "MarcoPoloTest", :plocal)
    assert {:ok, false} = db_exists?(c, "nonexistent", :plocal)
  end

  @tag :connected_to_server
  test "create_db/4 with a database that doens't exist yet", %{conn: c} do
    assert :ok = create_db(c, "MarcoPoloTestGenerated", :document, :plocal)
  end

  @tag :connected_to_server
  test "create_db/4 with a database that already exists", %{conn: c} do
    assert {:error, %Error{} = err} = create_db(c, "MarcoPoloTest", :document, :plocal)
    assert [{exception, msg}] = err.errors
    assert exception == "com.orientechnologies.orient.core.exception.ODatabaseException"
    assert msg       =~ "Database named 'MarcoPoloTest' already exists:"
  end

  @tag :connected_to_server
  test "drop_db/3 with an existing database", %{conn: c} do
    assert :ok = drop_db(c, "MarcoPoloToDrop", :memory)
  end

  @tag :connected_to_server
  test "drop_db/3 with a non-existing database", %{conn: c} do
    expected_ex = "com.orientechnologies.orient.core.exception.OStorageException"

    assert {:error, %Error{} = err} = drop_db(c, "Nonexistent", :plocal)
    assert [{^expected_ex, msg}] = err.errors
    assert msg =~ "Database with name 'Nonexistent' does not exist"
  end

  test "db_reload/1", %{conn: c} do
    assert :ok = db_reload(c)
  end

  test "db_size/1", %{conn: c} do
    assert {:ok, size} = db_size(c)
    assert is_integer(size)
    assert size >= 0
  end

  test "db_countrecords/1", %{conn: c} do
    assert {:ok, nrecords} = db_countrecords(c)
    assert is_integer(nrecords)
    assert nrecords >= 0
  end

  test "load_record/4: loading a document", %{conn: c} do
    rid = TestHelpers.record_rid("record_load")

    {:ok, {record, _}} = load_record(c, rid, fetch_plan: "*:-1")

    assert %Document{} = record
    assert record.version == 1
    assert record.class   == "Schemaless"
    assert record.fields  == %{"name" => "record_load", "f" => "foo"}

    rid = TestHelpers.record_rid("schemaless_record_load")

    {:ok, {record, _}} = load_record(c, rid)
    assert record.version == 1
    assert record.class == "Schemaful"
    assert record.fields == %{"myString" => "record_load"}
  end

  test "load_record/4 using the :if_version_not_latest option", %{conn: c} do
    rid = TestHelpers.record_rid("record_load")
    assert {:ok, {nil, _}} = load_record(c, rid, version: 1, if_version_not_latest: true)
  end

  test "delete_record/3", %{conn: c} do
    version = 1
    rid = TestHelpers.record_rid("record_delete")

    # Wrong version causes no deletions.
    assert {:ok, false} = delete_record(c, rid, version + 100)

    assert {:ok, true}  = delete_record(c, rid, version)
    assert {:ok, false} = delete_record(c, rid, version)
  end

  test "create_record/3: creating a record (document) synchronously", %{conn: c} do
    cluster_id = TestHelpers.cluster_id("schemaless")
    record = %Document{class: "Schemaless", fields: %{"foo" => "bar"}}

    {:ok, {rid, version}} = create_record(c, cluster_id, record)

    assert %RID{cluster_id: ^cluster_id} = rid
    assert is_integer(version)
  end

  test "create_record/3: creating a record (blob) synchronously", %{conn: c} do
    cluster_id = TestHelpers.cluster_id("schemaless_with_binary_records")
    record = %BinaryRecord{contents: <<84, 41>>}

    {:ok, {rid, version}} = create_record(c, cluster_id, record)

    assert %RID{cluster_id: ^cluster_id} = rid
    assert is_integer(version)
  end

  test "create_record/3: using the :no_response option", %{conn: c} do
    cluster_id = TestHelpers.cluster_id("schemaless")
    record = %Document{class: "Schemaless", fields: %{"foo" => "bar"}}

    :ok = create_record(c, cluster_id, record, no_response: true)
  end

  test "update_record/6 synchronously", %{conn: c} do
    rid = TestHelpers.record_rid("record_update")
    new_doc = %Document{class: "Schemaless", fields: %{f: "bar"}}

    assert {:ok, new_version} = update_record(c, rid, 1, new_doc, true)
    assert is_integer(new_version)
  end

  test "update_record/6 asynchronously (:no_response option)", %{conn: c} do
    rid = TestHelpers.record_rid("record_update")
    new_doc = %Document{class: "Schemaless", fields: %{f: "baz"}}

    assert :ok = update_record(c, rid, 1, new_doc, true, no_response: true)
  end

  test "command/3: SELECT query without a WHERE clause", %{conn: c} do
    {:ok, %{response: records}} = command(c, "SELECT FROM Schemaless", fetch_plan: "*:0")

    assert Enum.find(records, fn record ->
      assert %Document{} = record
      assert record.class == "Schemaless"
      record.fields["name"] == "record_load"
    end)
  end

  test "command/3: SELECT query with a WHERE clause", %{conn: c} do
    cmd = "SELECT FROM Schemaless WHERE name = 'record_load' LIMIT 1"
    res = command(c, cmd, fetch_plan: "*:-1")

    assert {:ok, %{response: [%Document{} = record]}} = res
    assert record.fields["name"] == "record_load"
  end

  test "command/3: SELECT query with named parameters", %{conn: c} do
    cmd    = "SELECT FROM Schemaless WHERE name = :name"
    params = %{"name" => "record_load"}
    res    = command(c, cmd, fetch_plan: "*:-1", params: params)

    assert {:ok, %{response: [%Document{} = record]}} = res
    assert record.fields["name"] == "record_load"
  end

  test "command/3: SELECT query with positional parameters", %{conn: c} do
    cmd = "SELECT FROM Schemaless WHERE name = ? AND f = ?"
    params = ["record_load", "foo"]
    res = command(c, cmd, params: params)

    assert {:ok, %{response: [%Document{} = doc]}} = res
    assert doc.fields["name"] == "record_load"
    assert doc.fields["f"] == "foo"
  end

  test "command/3: INSERT query inserting multiple records", %{conn: c} do
    cmd = "INSERT INTO Schemaless(my_field) VALUES ('value1'), ('value2')"

    assert {:ok, %{response: [r1, r2]}} = command(c, cmd)
    assert r1.fields["my_field"] == "value1"
    assert r2.fields["my_field"] == "value2"
  end

  test "command/3: miscellaneous commands", %{conn: c} do
    assert {:ok, %{}} = command(c, "CREATE CLUSTER misc_tests ID 1234")
    assert {:ok, %{}} = command(c, "CREATE CLASS MiscTests CLUSTER 1234")
    assert {:ok, %{}} = command(c, "CREATE PROPERTY MiscTests.foo DATETIME")
    assert {:ok, %{response: nil}} = command(c, "DROP PROPERTY MiscTests.foo")
    assert {:ok, %{response: true}} = command(c, "DROP CLASS MiscTests")
    assert {:ok, %{response: false}} = command(c, "DROP CLUSTER misc_tests")
    assert {:ok, %{response: false}} = command(c, "DROP CLUSTER misc_tests")
  end

  test "transaction/3: creating records", %{conn: c} do
    cluster_id = TestHelpers.cluster_id("schemaless")

    operations = [
      {:create, %Document{class: "Schemaless", fields: %{"name" => "inside_transaction"}}},
      {:create, %Document{class: "Schemaless", fields: %{"name" => "other_inside_transaction"}}},
    ]

    assert {:ok, %{created: created, updated: []}} = transaction(c, operations)

    assert [{%RID{cluster_id: ^cluster_id}, v1}, {%RID{cluster_id: ^cluster_id}, v2}] = created
    assert is_integer(v1)
    assert is_integer(v2)
  end

  test "transaction/3: updating/deleting a record with no :version raises", %{conn: c} do
    doc = %Document{version: nil, rid: %RID{cluster_id: 1, position: 1}}

    assert_raise Error, fn ->
      transaction(c, [{:delete, doc}])
    end
  end

  @tag :scripting
  test "script/4", %{conn: c} do
    script = """
    db.command('CREATE CLASS ScriptTest');

    for (i = 1; i <= 3; i++) {
      db.command('INSERT INTO ScriptTest(foo) VALUES("test' + i + '")');
    }
    """

    assert {:ok, _} = script(c, "Javascript", script)

    {:ok, %{response: records}} = command(c, "SELECT FROM ScriptTest", fetch_plan: "")
    records = Enum.map(records, fn(%Document{fields: %{"foo" => value}}) -> value end)

    assert records == ~w(test1 test2 test3)
  end

  test "working with embedded lists in schemaful classes", %{conn: c} do
    class = "WorkingWithLists"

    {:ok, %{response: cluster_id}} = command(c, "CREATE CLASS #{class}")
    {:ok, _} = command(c, "CREATE PROPERTY #{class}.list EMBEDDEDLIST")

    doc = %Document{class: class, fields: %{"l" => [1, "foo", 3.14]}}

    assert {:ok, {%RID{} = rid, _version}} = create_record(c, cluster_id, doc)

    assert {:ok, {loaded_doc, _}} = load_record(c, rid)
    assert loaded_doc.fields == doc.fields
  end

  test "creating and then updating a record", %{conn: c} do
    cmd = "INSERT INTO Schemaless(name, f) VALUES ('create and update', 'foo')"
    {:ok, %{response: %Document{} = doc}} = command(c, cmd)

    assert doc.fields == %{"name" => "create and update", "f" => "foo"}

    cmd = "UPDATE Schemaless SET f = 'bar' WHERE name = 'create and update'"
    {:ok, %{response: new_version}} = command(c, cmd)

    {:ok, {new_doc, _}} = load_record(c, doc.rid)

    assert new_doc.fields["f"] == "bar"
    assert is_integer(new_version)
  end

  test "working with nested embedded docs in schemaless classes", %{conn: c} do
    cluster_id = TestHelpers.cluster_id("schemaless")

    doc = %Document{class: "Schemaless", fields: %{
      nested: %{doc: %Document{fields: %{"foo" => "bar"}}}
    }}

    {:ok, {rid, _vsn}}  = create_record(c, cluster_id, doc)
    {:ok, {loaded_doc, _}} = load_record(c, rid)

    assert %{"nested" => nested} = loaded_doc.fields
    assert %{"doc" => %Document{} = nested_doc} = nested
    assert nested_doc.fields["foo"] == "bar"
  end

  test "creating and deleting a record using no_response operations", %{conn: c} do
    cluster_id = TestHelpers.cluster_id("schemaless")
    query = "SELECT FROM Schemaless WHERE name = 'no response ops'"
    fields = %{"foo" => "bar", "name" => "no response ops"}
    doc = %Document{class: "Schemaless", fields: fields}

    assert :ok = create_record(c, cluster_id, doc, no_response: true)

    assert {:ok, %{response: [loaded_doc]}} = command(c, query, fetch_plan: "*:-1")

    assert loaded_doc.class == "Schemaless"
    assert loaded_doc.fields == fields

    assert :ok = delete_record(c, loaded_doc.rid, loaded_doc.version, no_response: true)
    assert {:ok, %{response: []}} = command(c, query, fetch_plan: "*:-1")
  end

  test "unknown property ids are handled automatically with command/3", %{conn: c} do
    {:ok, _} = command(c, "CREATE CLASS UnknownPropertyIdCommand")
    {:ok, _} = command(c, "CREATE PROPERTY UnknownPropertyIdCommand.i SHORT")

    insert_query = "INSERT INTO UnknownPropertyIdCommand(i) VALUES (30)"
    assert {:ok, %{response: record}} = command(c, insert_query)
    assert %Document{} = record
    assert record.class   == "UnknownPropertyIdCommand"
    assert record.version == 1
    assert record.fields  == %{"i" => 30}
  end

  test "unknown property ids are handled automatically with load_record/3", %{conn: c} do
    # TODO: affected by the cluster_id changes in 2.2.0-beta.

    {:ok, %{response: _cluster_id}} =
      command(c, "CREATE CLASS UnknownPropertyIdRecordLoad")
    {:ok, _} =
      command(c, "CREATE PROPERTY UnknownPropertyIdRecordLoad.i SHORT")

    {:ok, %{response: %Document{rid: rid}}} =
      command(c, "INSERT INTO UnknownPropertyIdRecordLoad(i) VALUES (1)")

    assert {:ok, {loaded_doc, _}} = load_record(c, rid)
    assert loaded_doc.class == "UnknownPropertyIdRecordLoad"
    assert loaded_doc.fields == %{"i" => 1}
  end

  test "index management", %{conn: c} do
    assert {:ok, _} = command(c, "CREATE INDEX myIndex UNIQUE STRING")
    assert {:ok, _} = command(c, "INSERT INTO index:myIndex (key,rid) VALUES ('foo',#0:1)")

    query = "SELECT FROM index:myIndex WHERE key = 'foo'"
    assert {:ok, %{response: [%Document{fields: %{"key" => "foo", "rid" => rid}}]}}
           = command(c, query, fetch_plan: "*:-1")

    assert rid == %RID{cluster_id: 0, position: 1}
  end

  test "expiring timeouts when connecting" do
    assert {:error, :timeout} = start_link(connection: :server,
                                           user: TestHelpers.user(),
                                           password: TestHelpers.password(),
                                           timeout: 0)
  end

  test "expiring timeouts when performing operations", %{conn: c} do
    assert {:timeout, _} = catch_exit(command(c, "SELECT FROM Schemaless", fetch_plan: "*:0", timeout: 0))
  end

  test "creating and loading a binary record (blob)", %{conn: c} do
    cluster_id = TestHelpers.cluster_id("schemaless_with_binary_records")
    blob = %BinaryRecord{contents: <<91, 23>>}

    assert {:ok, {%RID{} = rid, _}} = create_record(c, cluster_id, blob)
    assert {:ok, {%BinaryRecord{} = record, _}} = load_record(c, rid)
    assert record.contents == <<91, 23>>
  end

  test "bad ops (server op on the db or viceversa) make the GenServer exit" do
    Process.flag :trap_exit, true
    {:ok, c} = start_link(user: TestHelpers.user, password: TestHelpers.password, connection: :server)

    silence_log fn ->
      msg = "must be connected to a database to perform operation db_reload"
      assert {{%Error{message: ^msg}, _}, _} = catch_exit(db_reload(c))
    end
  end

  # Tagged as >2.1 because 2.0 has a bunch of bugs with the SQL parser and
  # basically `CREATE EDGE FROM ? TO ?` doesn't work (with an error like
  # "Argument '?' is not a RecordId in form of string. Format must be:
  # <cluster-id>:<cluster-position>").
  @tag min_orientdb_version: "2.1.0"
  test "working with graphs", %{conn: c} do
    {:ok, _} = command(c, "CREATE CLASS Person EXTENDS V")
    {:ok, _} = command(c, "CREATE CLASS Restaurant EXTENDS V")
    {:ok, _} = command(c, "CREATE CLASS Eat EXTENDS E")

    assert {:ok, %{response: %Document{} = jane}}
           = command(c, "CREATE VERTEX Person SET name = 'Jane'")
    assert {:ok, %{response: %Document{} = pizza_place}}
           = command(c, "CREATE VERTEX Restaurant SET name = 'Pizza place'")

    # Let's assert we have the correct documents.
    assert jane.fields["name"] == "Jane"
    assert pizza_place.fields["name"] == "Pizza place"

    assert {:ok, %{response: [edge]}}
           = command(c, "CREATE EDGE Eat FROM ? to ?", params: [jane.rid, pizza_place.rid])
    assert edge.fields["in"] == pizza_place.rid
    assert edge.fields["out"] == jane.rid

    assert {:ok, %{response: [doc]}}
           = command(c, "SELECT IN() FROM Restaurant WHERE name = 'Pizza place'")
    assert doc.fields["IN"] == {:link_list, [jane.rid]}

    assert {:ok, %{response: [doc]}}
           = command(c, "SELECT OUT() FROM Person WHERE name = 'Jane'")
    assert doc.fields["OUT"] == {:link_list, [pizza_place.rid]}
  end

  test "nested queries", %{conn: c} do
    assert {:ok, %{response: [%Document{} = doc]}}
           = command(c, "SELECT * FROM (SELECT * FROM Schemaless) WHERE name = 'record_load'")
    assert doc.fields["name"] == "record_load"
  end

  test "transactions", %{conn: c} do
    # TODO: affected by the cluster_id changes in 2.2.0-beta.

    {:ok, %{response: _cluster_id}} =
      command(c, "CREATE CLASS TransactionsTest")
    {:ok, %{response: [_doc1, doc2, doc3]}} =
      command(c, "INSERT INTO TransactionsTest(f) VALUES (1), (2), (3)")

    doc2 = %{doc2 | fields: Map.update!(doc2.fields, "f", &(&1 * 100))}

    operations = [
      {:create, %Document{class: "TransactionsTest", fields: %{"f" => 4}}},
      {:update, doc2},
      {:create, %Document{class: "TransactionsTest", fields: %{"f" => 5}}},
      {:delete, doc3},
    ]

    assert {:ok, %{created: created, updated: updated}} = transaction(c, operations)

    assert [{%RID{}, _}, {%RID{}, _}] = created
    assert [{%RID{}, _}] = updated

    # Let's check the created records have actually been created.
    assert {:ok, %{response: [_, _]}}
           = command(c, "SELECT FROM TransactionsTest WHERE f = 4 OR f = 5")

    # Let's check the record to update has been updated.
    assert {:ok, %{response: [_updated_doc]}}
           = command(c, "SELECT FROM TransactionsTest WHERE f = 200")

    # Let's check that doc3 has been deleted since OrientDB doesn't send an ack
    # for deletions in transactions.
    assert {:ok, %{response: []}} = command(c, "SELECT FROM TransactionsTest WHERE f = 3")
  end

  test "unknown property ids after the first record", %{conn: c} do
    # TODO: affected by the cluster_id changes in 2.2.0-beta.

    {:ok, %{response: _cluster_id}} =
      command(c, "CREATE CLASS UnknownPropertyIds")

    {:ok, %{response: %Document{rid: rid1}}} =
      command(c, "INSERT INTO UnknownPropertyIds(i) VALUES (1)")

    {:ok, %{response: _property_id}} =
      command(c, "CREATE PROPERTY UnknownPropertyIds.str STRING")

    {:ok, %{response: %Document{rid: rid2}}} =
      command(c, "INSERT INTO UnknownPropertyIds(i, str) VALUES (2, 'value')")

    assert {:ok, %{response: [doc1, doc2]}}
           = command(c, "SELECT FROM UnknownPropertyIds ORDER BY i ASC")
    assert doc1.rid == rid1
    assert doc2.rid == rid2
  end

  test "fetch plans", %{conn: c} do
    {:ok, _} = command(c, "CREATE CLASS FetchingPlans")
    {:ok, %{response: mother}} =
      command(c, "INSERT INTO FetchingPlans(name) VALUES ('mother')")
    {:ok, %{response: father}} =
      command(c, "INSERT INTO FetchingPlans(name) VALUES ('father')")

    params = ["child", father.rid, mother.rid]
    {:ok, %{response: child}} =
      command(c, "INSERT INTO FetchingPlans(name, father, mother) VALUES (?, ?, ?)", params: params)

    assert {:ok, resp} =
      command(c, "SELECT FROM FetchingPlans WHERE name = 'child'", fetch_plan: "mother:0")

    assert resp.response == [child]
    assert FetchPlan.resolve_links(child.fields["mother"], resp.linked_records)
           == {:ok, mother}
    assert FetchPlan.resolve_links(child.fields["father"], resp.linked_records)
           == :error
  end

  @tag :scripting
  test "batch transaction in a script with the SQL langauge (committing)", %{conn: c} do
    {:ok, _} = command(c, "CREATE CLASS City EXTENDS V")
    {:ok, _} = command(c, "INSERT INTO City(name) VALUES ('London')")

    script = """
    BEGIN
    LET $person = CREATE VERTEX V SET first_name = 'Luke'
    LET $city = SELECT FROM City WHERE name = 'London'
    LET $edge = CREATE EDGE E FROM $person TO $city SET name = 'lives'
    COMMIT RETRY 100
    RETURN $edge
    """

    assert {:ok, %{response: [edge]}} = script(c, "SQL", script)
    assert edge.class == "E"
    assert edge.fields["name"] == "lives"
  end

  @tag :scripting
  test "batch transaction in a script with the SQL language (rolling back)", %{conn: c} do
    {:ok, _} = command(c, "CREATE CLASS Rollbacks")

    script = """
    BEGIN
    LET person = INSERT INTO Rollbacks(name) VALUES ('Luke')
    ROLLBACK
    RETURN null
    """

    assert {:ok, %{response: nil}} = script(c, "SQL", script)
    assert {:ok, %{response: []}} = command(c, "SELECT FROM Rollbacks")
  end

  @tag :live_query
  @tag min_orientdb_version: "2.1.0"
  test "live_query/4", %{conn: c} do
    {:ok, _} = command(c, "CREATE CLASS LiveQuerying")

    assert {:ok, token} = live_query(c, "LIVE SELECT FROM LiveQuerying", self())
    assert is_integer(token)

    {:ok, _} = command(c, "INSERT INTO LiveQuerying(text) VALUES ('test1'), ('test2')")
    assert_receive {:orientdb_live_query, ^token, {:create, doc}}, 1_000
    assert %Document{class: "LiveQuerying", rid: %RID{}, fields: %{"text" => "test1"}} = doc

    assert_receive {:orientdb_live_query, ^token, {:create, doc}}, 1_000
    assert %Document{class: "LiveQuerying", rid: %RID{}, fields: %{"text" => "test2"}} = doc

    {:ok, _} = command(c, "UPDATE LiveQuerying SET text = 'updated' WHERE text = 'test1'")
    assert_receive {:orientdb_live_query, ^token, {:update, doc}}, 1_000
    assert doc.fields["text"] == "updated"

    {:ok, _} = command(c, "DELETE FROM LiveQuerying WHERE text = 'updated'")
    assert_receive {:orientdb_live_query, ^token, {:delete, doc}}, 1_000
    assert doc.fields["text"] == "updated"

    assert :ok = live_query_unsubscribe(c, token)
    assert_receive {:orientdb_live_query, ^token, :unsubscribed}

    {:ok, _} = command(c, "INSERT INTO LiveQuerying(text) VALUES ('unsubscribed')")
    refute_receive {:orientdb_live_query, _, _}
  end

  @tag :no_started_connection
  @tag :ssl
  test "SSL connection" do
    assert {:ok, conn} = start_link(
      connection: :server,
      user: TestHelpers.user(),
      password: TestHelpers.password(),
      ssl: true,
      ssl_opts: [cacertfile: to_char_list(TestHelpers.cacertfile())]
    )

    assert db_exists?(conn, "MarcoPoloTest", :plocal) == {:ok, true}

    stop(conn)
  end

  defp assert_pid_will_die(pid) do
    if Process.alive?(pid) do
      :timer.sleep(20)
      assert_pid_will_die(pid)
    else
      refute Process.alive?(pid)
    end
  end

  defp stop_conn_blocking(conn) do
    :ok = stop(conn)
    assert_pid_will_die(conn)
  end

  # TODO: replace with ExUnit.CaptureIO once we can depend on ~> 1.1
  defp silence_log(fun) do
    Logger.remove_backend :console
    fun.()
  after
    Logger.add_backend :console, flush: true
  end
end
