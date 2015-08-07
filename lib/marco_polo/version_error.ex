defmodule MarcoPolo.VersionError do
  defexception [:message]

  def exception(msg) when is_binary(msg) do
    %__MODULE__{message: msg}
  end
end
