defmodule MarcoPolo.VersionError do
  @moduledoc """
  Raised when there's a version mismatch with the OrientDB server.
  """

  defexception [:message]

  def exception(msg) when is_binary(msg) do
    %__MODULE__{message: msg}
  end
end
