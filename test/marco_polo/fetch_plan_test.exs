defmodule MarcoPolo.FetchPlanTest do
  use ExUnit.Case

  import MarcoPolo.FetchPlan, only: [follow_link: 2]
  alias MarcoPolo.RID
  alias MarcoPolo.Document, as: Doc

  @target_rid %RID{cluster_id: 31, position: 415}

  test "follow_link/2: single link which can be followed" do
    doc = %Doc{rid: @target_rid, class: "Foo"}
    linked = make_linked([
      rid_and_doc(@target_rid, doc.class),
      rid_and_doc(rid(0, 0), "Bar"),
    ])

    assert follow_link(@target_rid, linked) == {:ok, doc}
  end

  test "follow_link/2: single link which cannot be followed" do
    doc = %Doc{rid: @target_rid, class: "Foo"}
    linked = make_linked([rid_and_doc(rid(-1, -1), %Doc{class: "Bar"})])

    assert follow_link(@target_rid, linked) == :error
  end

  test "follow_link/2: lists of links with all the right links" do
    rids = [rid(0, 0), rid(0, 1)]
    linked = make_linked([
      rid_and_doc(rid(0, 1), "Foo"),
      rid_and_doc(rid(0, 99), "Bar"),
      rid_and_doc(rid(0, 0), "Baz"),
    ])

    assert {:ok, [%Doc{class: "Baz"}, %Doc{class: "Foo"}]}
           = follow_link(rids, linked)
  end

  test "follow_link/2: list of links with missing links" do
    rids = [rid(0, 0)]
    linked = make_linked([rid_and_doc(rid(99, 99), "Foo")])
    assert follow_link(rids, linked) == :error
  end

  test "follow_link/2: maps of links with all the right links" do
    rids = %{"foo" => rid(0, 0), "bar" => rid(0, 1)}
    linked = make_linked([
      rid_and_doc(rid(0, 0), "Foo"),
      rid_and_doc(rid(0, 1), "Bar"),
      rid_and_doc(rid(0, 100), "Wat"),
    ])

    assert %{"foo" => %Doc{class: "Foo"}, "bar" => %Doc{class: "Bar"}}
           = follow_link(rids, linked)
  end

  test "follow_link/2: maps of links with missing links" do
    rids = %{"foo" => rid(0, 0)}
    linked = make_linked([
      rid_and_doc(rid(0, 100), "Wat"),
    ])

    assert follow_link(rids, linked) == :error
  end

  test "follow_link/2: embedded link bag with all the right links" do
    rids = {:link_bag, [rid(0, 0), rid(0, 1)]}
    linked = make_linked([
      rid_and_doc(rid(0, 1), "Foo"),
      rid_and_doc(rid(0, 99), "Bar"),
      rid_and_doc(rid(0, 0), "Baz"),
    ])

    assert {:ok, {:link_bag, [%Doc{class: "Baz"}, %Doc{class: "Foo"}]}}
           = follow_link(rids, linked)
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
