defmodule MarcoPolo.BinaryRecord do
  @type t :: %__MODULE__{
    contents: binary,
    rid: MarcoPolo.RID.t,
    version: non_neg_integer,
  }

  defstruct [:contents, :rid, :version]
end
