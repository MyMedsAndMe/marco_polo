defmodule MarcoPolo.RID do
  @moduledoc """
  A struct that represents an OrientDB record id (RID).

  OrientDB identifies records with a unique id, called the record id (RID). A
  RID consists of the id of the cluster the record is in and the position of the
  record in that cluster. The fields for the `MarcoPolo.RID` struct represent
  exactly this:

    * `:cluster_id` - the id of the cluster
    * `:position` - the position in the cluster

  For more information on RIDs, refer to the [OrientDB
  docs](http://orientdb.com/docs/last/Tutorial-Record-ID.html).
  """

  @type t :: %__MODULE__{
    cluster_id: non_neg_integer,
    position: non_neg_integer,
  }

  defstruct [:cluster_id, :position]

  defimpl Inspect do
    def inspect(%MarcoPolo.RID{cluster_id: cid, position: pos}, _opts) do
      "#MarcoPolo.RID<##{cid}:#{pos}>"
    end
  end
end
