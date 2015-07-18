defmodule MarcoPolo.Error do
  defexception message: nil, errors: []

  @type t :: %__MODULE__{
    message: binary,
    errors: [{binary, binary}],
  }

  def message(%{errors: errors}) do
    "OrientDB error:\n" <> Enum.map_join(errors, "\n", fn {type, msg} ->
      "(#{type}) #{msg}"
    end)
  end
end
