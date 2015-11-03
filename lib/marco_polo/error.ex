defmodule MarcoPolo.Error do
  @moduledoc """
  Raised for errors happening on the server.
  """

  defexception message: nil, errors: []

  @type t :: %__MODULE__{
    message: binary,
    errors: [{binary, binary}],
  }

  def exception(msg) when is_binary(msg) do
    %__MODULE__{message: msg}
  end

  def message(%{message: msg}) when is_binary(msg) do
    msg
  end

  def message(%{errors: errors, message: nil}) do
    "OrientDB error:\n" <> Enum.map_join(errors, "\n", fn {type, msg} ->
      "(#{type}) #{msg}"
    end)
  end
end
