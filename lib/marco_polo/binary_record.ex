defmodule MarcoPolo.BinaryRecord do
  @moduledoc """
  Struct that represents OrientDB binary data (`ORecordBytes`).

  This struct has the following fields:

    * `:rid` - the record id of the record (as a `MarcoPolo.RID`). It's `nil`
      for new records (not yet stored on the database).
    * `:contents` - the binary content of the record.
    * `:version` - the version of the record.

  """

  @type t :: %__MODULE__{
    contents: binary,
    rid: MarcoPolo.RID.t,
    version: non_neg_integer,
  }

  defstruct [:contents, :rid, :version]
end
