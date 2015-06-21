defmodule MarcoPolo.Error do
  defexception message: nil, errors: []

  @type t :: %__MODULE__{message: binary, errors: [{binary, binary}]}

  @doc """
  Builds a `MarcoPolo.Error` struct from a list of `{class, msg}` errors.

  The message is built from the list of `errors`; the `:errors` field is just
  `errors`.
  """
  @spec from_errors([{binary, binary}]) :: t
  def from_errors(errors) do
    exception_msg = "OrientDB errors:\n" <> format_errors(errors)
    %__MODULE__{message: exception_msg, errors: errors}
  end

  defp format_errors(errors) do
    Enum.map_join errors, "\n", fn {class, message} ->
      "(#{class}) #{message}"
    end
  end
end
