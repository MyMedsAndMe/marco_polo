defmodule MarcoPolo.FetchPlan do
  alias MarcoPolo.RID

  def follow_link(%RID{} = rid, linked) do
    Dict.fetch(linked, rid)
  end

  def follow_link(rids, linked) when is_list(rids) do
    catching_missing fn ->
      followed = Enum.map rids, fn(%RID{} = rid) ->
        case follow_link(rid, linked) do
          {:ok, doc} -> doc
          :error     -> throw(:missing_link)
        end
      end

      {:ok, followed}
    end
  end

  def follow_link(rids, linked) when is_map(rids) do
    catching_missing fn ->
      for {key, rid} <- rids, into: %{} do
        case follow_link(rid, linked) do
          {:ok, doc} -> {key, doc}
          :error     -> throw(:missing_link)
        end
      end
    end
  end

  def follow_link({:link_bag, rids}, linked) do
    case follow_link(rids, linked) do
      {:ok, res} -> {:ok, {:link_bag, res}}
      :error     -> :error
    end
  end

  defp catching_missing(fun) do
    try do
      fun.()
    catch
      :missing_link -> :error
    end
  end
end
