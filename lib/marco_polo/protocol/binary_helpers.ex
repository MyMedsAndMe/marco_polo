defmodule MarcoPolo.Protocol.BinaryHelpers do
  @moduledoc false

  # This module provides some helpers to be used when pattern matching with the
  # binary syntax. For example, the `bytes(n)` helper can be used to specify a
  # number of bytes instead of using bits:
  #
  #     <<value :: bytes(4)>> = data
  #
  # These helpers are tied to OrientDB's binary protocol.

  defmacro bytes(n) do
    quote do
      unquote(n) * 8
    end
  end

  defmacro short do
    quote do: unquote(bytes(2))-signed
  end

  defmacro int do
    quote do: unquote(bytes(4))-signed
  end

  defmacro long do
    quote do: unquote(bytes(8))-signed
  end
end
