defmodule MarcoPoloMiscTest do
  use ExUnit.Case, async: true

  import MarcoPolo
  alias MarcoPolo.Document
  alias MarcoPolo.RID

  setup do
    {:ok, conn} = MarcoPolo.start_link(
      connection: {:db, "MarcoPoloTest", "plocal"},
      user: TestHelpers.user(),
      password: TestHelpers.password()
    )

    {:ok, %{conn: conn}}
  end

  test "working with embedded lists in schemaful classes", %{conn: c} do
    class = "WorkingWithLists"

    {:ok, [cluster_id]} = command(c, "CREATE CLASS #{class}")
    {:ok, _}            = command(c, "CREATE PROPERTY #{class}.list EMBEDDEDLIST")

    fetch_schema(c)

    doc = %Document{class: class, fields: %{"l" => [1, "foo", 3.14]}}

    assert {:ok, {%RID{} = rid, _version}} = create_record(c, cluster_id, doc)

    assert {:ok, [loaded_doc]} = load_record(c, rid, "*:-1")
    assert loaded_doc.fields == doc.fields
  end

  test "working with nested embedded docs in schemaless classes", %{conn: c} do
    cluster_id = TestHelpers.cluster_id("schemaless")

    doc = %Document{class: "Schemaless", fields: %{
      nested: %{doc: %Document{fields: %{"foo" => "bar"}}}
    }}

    {:ok, {rid, _vsn}}  = create_record(c, cluster_id, doc)
    {:ok, [loaded_doc]} = load_record(c, rid, "*:-1")

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
end
