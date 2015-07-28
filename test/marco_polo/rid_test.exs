defmodule MarcoPolo.RIDTest do
  use ExUnit.Case

  alias MarcoPolo.RID

  test "RIDs are inspectable" do
    rid = %RID{cluster_id: 9, position: 143}
    assert inspect(rid) == "#MarcoPolo.RID<#9:143>"
  end
end
