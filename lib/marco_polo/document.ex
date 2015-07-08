defmodule MarcoPolo.Document do
  @moduledoc """
  Struct representing an OrientDB document (`ODocument`).

  Contains the following fields:

    * `:class` - the class of the document. Can be `nil` when the document has
      no class.
    * `:version` - the version of the document.
    * `:fields` - the fields of the document.
    * `:rid` - the record id of the document (as a `MarcoPolo.RID`). It's `nil`
      for new documents (not yet stored on the database).

  """

  @type t :: %__MODULE__{
    class: nil | String.t,
    version: nil | non_neg_integer,
    fields: %{},
    rid: nil | MarcoPolo.RID.t,
  }

  defstruct class: nil,
            version: nil,
            fields: %{},
            rid: nil
end
