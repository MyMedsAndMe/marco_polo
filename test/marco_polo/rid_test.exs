defmodule MarcoPolo.RIDTest do
  use ExUnit.Case, async: true

  test "RIDs are inspectable" do
    rid = %MarcoPolo.RID{cluster_id: 9, position: 143}
    assert inspect(rid) == "#MarcoPolo.RID<#9:143>"
  end
end
