defmodule MarcoPolo.FetchPlan do
  @moduledoc """
  Provides facilities for traversing links to OrientDB records.
  """

  alias MarcoPolo.RID

  defmodule RecordNotFoundError do
    @moduledoc """
    Raised when a record is not found in a set of linked records.
    """
    defexception [:message]
  end

  @doc """
  Transforms RIDs to OrientDB records based on a set of linked records.

  `linked` is a dict with RIDs as keys and OrientDB records
  (`MarcoPolo.Document` and `MarcoPolo.BinaryRecord` structs) as values. Each
  RID key is the RID of the record in the corresponding value. Usually, this
  dict is the set of linked records that some functions from the `MarcoPolo`
  return alongside the queried records; what records get in this set depends on
  the fetch plan used in the query.

  `rids` can be:

    * a single RID: this function returns the record in `linked` with that RID.
    * a list of RIDs: this function returns a list as long as `rids` where each
      RID has been replaced by the record with that RID in `linked`.
    * a map where the values are RIDs: this function returns a map with the same
      keys as `rids` but where each RID value has been replaced by the record
      with that RID in `linked`.

  When all the RIDs in `rids` are found in `linked`, then the response is always
  `{:ok, records}` where the result `records` depends on `rids` and is described
  in the list above. If one or more RIDs are not found in `linked`, then
  `:error` is returned.

  ## Examples

      iex> rid = %MarcoPolo.RID{cluster_id: 1, position: 10}
      iex> linked = HashDict.new
      ...>          |> Dict.put(rid, %MarcoPolo.Document{rid: rid, fields: %{"foo" => "bar"}})
      iex> {:ok, doc} = MarcoPolo.FetchPlan.resolve_links(rid, linked)
      iex> doc.fields
      %{"foo" => "bar"}
      iex> doc.rid == rid
      true

      iex> rids = [%MarcoPolo.RID{cluster_id: 1, position: 10},
      ...>         %MarcoPolo.RID{cluster_id: 1, position: 11}]
      iex> MarcoPolo.FetchPlan.resolve_links(rids, %{})
      :error

  """
  @spec resolve_links(RID.t, [MarcoPolo.rec]) :: {:ok, MarcoPolo.rec} | :error
  @spec resolve_links([RID.t], [MarcoPolo.rec]) :: {:ok, [MarcoPolo.rec]} | :error
  @spec resolve_links(%{term => RID.t}, [MarcoPolo.rec]) :: {:ok, %{term => MarcoPolo.rec}} | :error
  def resolve_links(rids, linked)

  def resolve_links(%RID{} = rid, linked) do
    Dict.fetch(linked, rid)
  end

  def resolve_links(rids, linked) when is_list(rids) do
    catch_missing fn ->
      followed = Enum.map rids, fn(%RID{} = rid) ->
        case resolve_links(rid, linked) do
          {:ok, record} -> record
          :error     -> throw(:missing_link)
        end
      end

      {:ok, followed}
    end
  end

  def resolve_links(rids, linked) when is_map(rids) do
    catch_missing fn ->
      map = for {key, rid} <- rids, into: %{} do
        case resolve_links(rid, linked) do
          {:ok, record} -> {key, record}
          :error     -> throw(:missing_link)
        end
      end

      {:ok, map}
    end
  end

  @doc """
  Transforms RIDs to OrientDB records based on a set of linked records, raising
  an exception for not found records.

  This function behaves exactly like `resolve_links/2`, except it returns the
  result directly (not as `{:ok, res}` but just as `res`) or raises a
  `MarcoPolo.FetchPlan.RecordNotFoundError` in case one of the RIDs is not
  found.

  ## Examples

      iex> rid = %MarcoPolo.RID{cluster_id: 1, position: 10}
      iex> linked = HashDict.new
      ...>          |> Dict.put(rid, %MarcoPolo.Document{rid: rid, fields: %{"foo" => "bar"}})
      iex> MarcoPolo.FetchPlan.resolve_links!(rid, linked).fields
      %{"foo" => "bar"}

  """
  @spec resolve_links!(RID.t, [MarcoPolo.rec]) :: MarcoPolo.rec
  @spec resolve_links!([RID.t], [MarcoPolo.rec]) :: [MarcoPolo.rec]
  @spec resolve_links!(%{term => RID.t}, [MarcoPolo.rec]) :: %{term => MarcoPolo.rec}
  def resolve_links!(rids, linked) do
    case resolve_links(rids, linked) do
      {:ok, res} ->
        res
      :error ->
        raise RecordNotFoundError, message: """
        the linked records don't include one of these RIDs:

        #{inspect rids}
        """
    end
  end

  defp catch_missing(fun) do
    try do
      fun.()
    catch
      :missing_link -> :error
    end
  end
end
