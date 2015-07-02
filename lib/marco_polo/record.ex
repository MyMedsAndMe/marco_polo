defmodule MarcoPolo.Record do
  @type t :: %__MODULE__{
    class: nil | String.t,
    version: non_neg_integer,
    fields: %{}
  }

  defstruct class: nil,
            version: nil,
            fields: %{}
end
