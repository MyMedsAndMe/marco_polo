defmodule MarcoPoloMiscTest do
  use ExUnit.Case
  @moduletag :integration

  import MarcoPolo
  alias MarcoPolo.Document
  alias MarcoPolo.BinaryRecord
  alias MarcoPolo.RID
  alias MarcoPolo.Error

  setup do
    {:ok, conn} = MarcoPolo.start_link(
      connection: {:db, "MarcoPoloTest", :document},
      user: TestHelpers.user(),
      password: TestHelpers.password()
    )

    on_exit fn -> MarcoPolo.stop(conn) end

    {:ok, %{conn: conn}}
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

    {:ok, %{response: [loaded_doc]}} = command(c, query, fetch_plan: "*:-1")

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
    {:ok, %{response: cluster_id}} = command(c, "CREATE CLASS UnknownPropertyIdRecordLoad")
    {:ok, _} = command(c, "CREATE PROPERTY UnknownPropertyIdRecordLoad.i SHORT")

    doc = %Document{class: "UnknownPropertyIdRecordLoad", fields: %{"i" => 1}}
    {:ok, {rid, _}} = create_record(c, cluster_id, doc)

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
    cluster_id = TestHelpers.cluster_id("schemaless")
    blob = %BinaryRecord{contents: <<91, 23>>}

    assert {:ok, {%RID{} = rid, _}} = create_record(c, cluster_id, blob)
    assert {:ok, {%BinaryRecord{} = record, _}} = load_record(c, rid)
    assert record.contents == <<91, 23>>
  end

  test "bad ops (server op on the db or viceversa) make the GenServer exit" do
    Process.flag :trap_exit, true
    {:ok, c} = start_link(user: TestHelpers.user, password: TestHelpers.password, connection: :server)

    msg = "must be connected to a database to perform operation db_reload"

    Logger.remove_backend(:console, flush: true)
    assert {{%Error{message: ^msg}, _}, _} = catch_exit(db_reload(c))
    Logger.add_backend(:console, flush: true)
  end

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
    {:ok, %{response: cluster_id}} =
      command(c, "CREATE CLASS TransactionsTest")
    {:ok, %{response: [doc1, doc2, doc3]}} =
      command(c, "INSERT INTO TransactionsTest(f) VALUES (1), (2), (3)")

    doc2 = %{doc2 | fields: Map.update!(doc2.fields, "f", &(&1 * 100))}

    operations = [
      {:create, %Document{class: "TransactionsTest", fields: %{"f" => 4}}},
      {:update, doc2},
      {:create, %Document{class: "TransactionsTest", fields: %{"f" => 5}}},
      {:delete, doc3},
    ]

    assert {:ok, %{created: created, updated: updated}} = transaction(c, operations)

    assert [{%RID{cluster_id: ^cluster_id}, _}, {%RID{cluster_id: ^cluster_id}, _}]
           = created
    assert [{%RID{cluster_id: ^cluster_id}, _}] = updated

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
    {:ok, %{response: cluster_id}} =
      command(c, "CREATE CLASS UnknownPropertyIds")

    {:ok, {rid1, _}} =
      create_record(c, cluster_id, %Document{class: "UnknownPropertyIds", fields: %{"i" => 1}})

    {:ok, _} =
      command(c, "CREATE PROPERTY UnknownPropertyIds.str STRING")

    {:ok, {rid2, _}} =
      create_record(c,
                    cluster_id,
                    %Document{class: "UnknownPropertyIds", fields: %{"i" => 2, "str" => "value"}})

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
    assert MarcoPolo.FetchPlan.follow_link(child.fields["mother"], resp.linked_records)
           == {:ok, mother}
    assert MarcoPolo.FetchPlan.follow_link(child.fields["father"], resp.linked_records)
           == :error
  end

  @tag :scripting
  test "batch transaction in a script with the SQL langauge (committing)", %{conn: c} do
    {:ok, _} = command(c, "CREATE CLASS City")
    {:ok, _} = command(c, "INSERT INTO City(name) VALUES ('London')")

    script = """
    BEGIN
    LET person = CREATE VERTEX V SET first_name = 'Luke'
    LET city = SELECT FROM City WHERE name = 'London'
    LET edge = CREATE EDGE E FROM $person TO $city SET name = 'lives'
    COMMIT RETRY 100
    RETURN $edge
    """

    assert {:ok, {[edge], _}} = script(c, "SQL", script)
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
end
