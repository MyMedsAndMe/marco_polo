defmodule MarcoPolo.Binary.Helpers do
  defmacro bytes(n) do
    quote do
      unquote(n) * 8
    end
  end

  defmacro short do
    quote do: 16-signed
  end

  defmacro int do
    quote do: 32-signed
  end

  defmacro long do
    quote do: 64-signed
  end
end
