defmodule MarcoPolo.UndecodedDocument do
  @moduledoc false

  # This struct represents an undecoded record which wasn't decoded because of a
  # missing property id. It has the `:version` and `:rid` fields since those
  # aren't stored in the record (with the schemaless serialization).

  @type t :: %__MODULE__{
    version: non_neg_integer,
    content: binary,
    rid: MarcoPolo.RID.t,
  }

  defstruct [:version, :content, :rid]
end
