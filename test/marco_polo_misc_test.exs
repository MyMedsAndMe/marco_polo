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

    {:ok, cluster_id} = command(c, "CREATE CLASS #{class}")
    {:ok, _} = command(c, "CREATE PROPERTY #{class}.list EMBEDDEDLIST")

    doc = %Document{class: class, fields: %{"l" => [1, "foo", 3.14]}}

    assert {:ok, {%RID{} = rid, _version}} = create_record(c, cluster_id, doc)

    assert {:ok, [loaded_doc]} = load_record(c, rid)
    assert loaded_doc.fields == doc.fields
  end

  test "creating and then updating a record", %{conn: c} do
    cmd = "INSERT INTO Schemaless(name, f) VALUES ('create and update', 'foo')"
    {:ok, %Document{} = doc} = command(c, cmd)

    assert doc.fields == %{"name" => "create and update", "f" => "foo"}

    cmd = "UPDATE Schemaless SET f = 'bar' WHERE name = 'create and update'"
    {:ok, new_version} = command(c, cmd)

    {:ok, [new_doc]} = load_record(c, doc.rid)

    assert new_doc.fields["f"] == "bar"
    assert is_integer(new_version)
  end

  test "working with nested embedded docs in schemaless classes", %{conn: c} do
    cluster_id = TestHelpers.cluster_id("schemaless")

    doc = %Document{class: "Schemaless", fields: %{
      nested: %{doc: %Document{fields: %{"foo" => "bar"}}}
    }}

    {:ok, {rid, _vsn}}  = create_record(c, cluster_id, doc)
    {:ok, [loaded_doc]} = load_record(c, rid)

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

    {:ok, [loaded_doc]} = command(c, query, fetch_plan: "*:-1")

    assert loaded_doc.class == "Schemaless"
    assert loaded_doc.fields == fields

    assert :ok = delete_record(c, loaded_doc.rid, loaded_doc.version, no_response: true)
    assert {:ok, []} = command(c, query, fetch_plan: "*:-1")
  end

  test "unknown property ids are handled automatically", %{conn: c} do
    {:ok, _} = command(c, "CREATE CLASS UnknownPropertyId")
    {:ok, _} = command(c, "CREATE PROPERTY UnknownPropertyId.i SHORT")

    insert_query = "INSERT INTO UnknownPropertyId(i) VALUES (30)"
    assert {:ok, %Document{} = record} = command(c, insert_query)
    assert record.class   == "UnknownPropertyId"
    assert record.version == 1
    assert record.fields  == %{"i" => 30}
  end

  test "index management", %{conn: c} do
    assert {:ok, _} = command(c, "CREATE INDEX myIndex UNIQUE STRING")
    assert {:ok, _} = command(c, "INSERT INTO index:myIndex (key,rid) VALUES ('foo',#0:1)")

    # TODO this fails with a fetch plan of *:-1 (fails with a timeout).
    query = "SELECT FROM index:myIndex WHERE key = 'foo'"
    assert {:ok, [%Document{fields: %{"key" => "foo", "rid" => rid}}]}
           = command(c, query, fetch_plan: "*:0")

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
    assert {:ok, [%BinaryRecord{} = record]} = load_record(c, rid)
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

    assert {:ok, %Document{} = jane} = command(c, "CREATE VERTEX Person SET name = 'Jane'")
    assert {:ok, %Document{} = pizza_place} = command(c, "CREATE VERTEX Restaurant SET name = 'Pizza place'")

    # Let's assert we have the correct documents.
    assert jane.fields["name"] == "Jane"
    assert pizza_place.fields["name"] == "Pizza place"

    assert {:ok, [edge]} = command(c, "CREATE EDGE Eat FROM ? to ?", params: [jane.rid, pizza_place.rid])

    assert edge.fields["in"] == pizza_place.rid
    assert edge.fields["out"] == jane.rid

    assert {:ok, [doc]} = command(c, "SELECT IN() FROM Restaurant WHERE name = 'Pizza place'")
    assert doc.fields["IN"] == {:link_list, [jane.rid]}

    assert {:ok, [doc]} = command(c, "SELECT OUT() FROM Person WHERE name = 'Jane'")
    assert doc.fields["OUT"] == {:link_list, [pizza_place.rid]}
  end

  test "nested queries", %{conn: c} do
    assert {:ok, [%Document{} = doc]} = command(c, "SELECT * FROM (SELECT * FROM Schemaless) WHERE name = 'record_load'")
    assert doc.fields["name"] == "record_load"
  end
end
