defmodule MarcoPolo.RID do
  @moduledoc """
  Struct that represents an OrientDB record id (RID).

  OrientDB identifies records with a unique id, called the record id (RID). A
  RID consists of the id of the cluster the record is and the position of the
  record in the cluster. The fields for the `MarcoPolo.RID` struct represent
  exactly this:

    * `:cluster_id` - the id of the cluster
    * `:position` - the position in the cluster

  For more information on RIDs, refer to the [OrientDB
  docs](http://orientdb.com/docs/2.0/orientdb.wiki/Tutorial-Record-ID.html).

  """

  @type t :: %__MODULE__{
    cluster_id: non_neg_integer,
    position: non_neg_integer,
  }

  defstruct [:cluster_id, :position]
end
