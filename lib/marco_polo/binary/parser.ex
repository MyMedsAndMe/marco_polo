defmodule MarcoPolo.Binary.Parser do
  @moduledoc false

  import MarcoPolo.Binary.Helpers

  # Basic data types.

  def parse_string(<<length :: bytes(4), rest :: binary>>) do
    byte_length = bytes(length)
    <<string :: bits-size(byte_length), rest :: binary>> = rest
    {string, rest}
  end

  def parse_short(<<i :: short, rest :: binary>>),
    do: {i, rest}

  def parse_int(<<i :: int, rest :: binary>>),
    do: {i, rest}

  def parse_long(<<i :: long, rest :: binary>>),
    do: {i, rest}

  def parse_boolean(<<0, rest :: binary>>),     do: {false, rest}
  def parse_boolean(<<_byte, rest :: binary>>), do: {true, rest}
end
