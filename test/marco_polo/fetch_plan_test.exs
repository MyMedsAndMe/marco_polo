defmodule MarcoPolo.FetchPlanTest do
  use ExUnit.Case

  doctest MarcoPolo.FetchPlan

  import MarcoPolo.FetchPlan, only: [resolve_links: 2, resolve_links!: 2]
  alias MarcoPolo.RID
  alias MarcoPolo.Document, as: Doc

  @target_rid %RID{cluster_id: 31, position: 415}

  test "resolve_links/2: single link which can be followed" do
    doc = %Doc{rid: @target_rid, class: "Foo"}
    linked = make_linked([
      rid_and_doc(@target_rid, doc.class),
      rid_and_doc(rid(0, 0), "Bar"),
    ])

    assert resolve_links(@target_rid, linked) == {:ok, doc}
  end

  test "resolve_links/2: single link which cannot be followed" do
    linked = make_linked([rid_and_doc(rid(-1, -1), "Bar")])
    assert resolve_links(@target_rid, linked) == :error
  end

  test "resolve_links/2: lists of links with all the right links" do
    rids = [rid(0, 0), rid(0, 1)]
    linked = make_linked([
      rid_and_doc(rid(0, 1), "Foo"),
      rid_and_doc(rid(0, 99), "Bar"),
      rid_and_doc(rid(0, 0), "Baz"),
    ])

    assert {:ok, [%Doc{class: "Baz"}, %Doc{class: "Foo"}]}
           = resolve_links(rids, linked)
  end

  test "resolve_links/2: list of links with missing links" do
    rids = [rid(0, 0)]
    linked = make_linked([rid_and_doc(rid(99, 99), "Foo")])
    assert resolve_links(rids, linked) == :error
  end

  test "resolve_links/2: maps of links with all the right links" do
    rids = %{"foo" => rid(0, 0), "bar" => rid(0, 1)}
    linked = make_linked([
      rid_and_doc(rid(0, 0), "Foo"),
      rid_and_doc(rid(0, 1), "Bar"),
      rid_and_doc(rid(0, 100), "Wat"),
    ])

    assert {:ok, %{"foo" => %Doc{class: "Foo"}, "bar" => %Doc{class: "Bar"}}}
           = resolve_links(rids, linked)
  end

  test "resolve_links/2: maps of links with missing links" do
    rids = %{"foo" => rid(0, 0)}
    linked = make_linked([
      rid_and_doc(rid(0, 100), "Wat"),
    ])

    assert resolve_links(rids, linked) == :error
  end

  test "resolve_links!/2: behaves like follow link but returns a RecordNotFound error" do
    rids = %{"foo" => rid(0, 0), "bar" => rid(0, 1)}
    linked = make_linked([
      rid_and_doc(rid(0, 0), "Foo"),
      rid_and_doc(rid(0, 1), "Bar"),
      rid_and_doc(rid(0, 100), "Wat"),
    ])

    assert %{"foo" => %Doc{class: "Foo"}, "bar" => %Doc{class: "Bar"}}
           = resolve_links!(rids, linked)

    error = assert_raise MarcoPolo.FetchPlan.RecordNotFoundError, fn ->
      resolve_links!(rid(99, 99), linked)
    end

    assert error.message =~ "the linked records don't include one of these RIDs"
  end

  defp rid_and_doc(rid, class) do
    {rid, %Doc{rid: rid, class: class}}
  end

  defp rid(cluster, pos) do
    %RID{cluster_id: cluster, position: pos}
  end

  defp make_linked(enum) do
    Enum.into(enum, HashDict.new)
  end
end
