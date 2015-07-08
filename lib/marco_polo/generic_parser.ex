defmodule MarcoPolo.GenericParser do
  @moduledoc false

  # Provides facilities for parsing binary data with support for incomplete
  # data.

  # This module provides functions for parsing binary data through given
  # *parsers* (which are just functions). What makes this module useful over
  # manually parsing these data is its declarativeness (you just list what data
  # you expect) as well as its support for incomplete data.

  # Incomplete data means data that ends before they can be fully parsed. For
  # example, an OrientDB long takes 8 bytes, so if you want to parse a long and
  # the binary contains less than 8 bytes than it's incomplete. Incomplete
  # responses are not handled in this module (which just returns `:incomplete`
  # when a response is incomplete), but on a higher level (the connection
  # server, which caches the incomplete data until it receives new data, then
  # tries to parse again).

  @typedoc """
  Type returned by the parsing functions in this module.
  """
  @type ok_or_incomplete :: {term, binary} | :incomplete

  @typedoc """
  A basic parser is just a function that takes a binary and returns `{value,
  rest}` or `:incomplete`; a parser can be a basic parser or a more complex
  parser usually based on basic ones.
  """
  @type parser :: (binary -> ok_or_incomplete)

  @doc """
  Parses `data` based on the given list of parsers. Returns `:incomplete` when
  the data is not enough to satisfy all `parsers`, `{value, rest}` otherwise.
  """
  @spec parse(binary, [parser]) :: ok_or_incomplete
  def parse(data, parsers)

  # You could basically call the parser directly on the data, but using
  # parse/1 makes sense because of nested parsing.
  def parse(data, parser) when is_binary(data) and is_function(parser, 1) do
    parser.(data)
  end

  def parse(data, parsers) when is_binary(data) and is_list(parsers) do
    parse(data, parsers, [])
  end

  defp parse(data, [parser|t], acc) do
    case parser.(data) do
      {value, rest} -> parse(rest, t, [value|acc])
      :incomplete   -> :incomplete
    end
  end

  defp parse(data, [], acc) do
    {Enum.reverse(acc), data}
  end

  @doc """
  Returns a parser that parses arrays.

  The returned parser will first parse the number of elements in the array from
  the given binary using the `nelems_fn` parser; then, it will parse elements
  using the `elem_parsers` parsers that number of times.
  """
  @spec array_parser(parser, [parser]) :: parser
  def array_parser(nelems_fn, elem_parsers) when is_function(nelems_fn, 1) do
    fn(data) ->
      case nelems_fn.(data) do
        :incomplete    -> :incomplete
        {nelems, rest} -> parse_array(rest, nelems, elem_parsers, [])
      end
    end
  end

  defp parse_array(data, 0, _parsers, acc) do
    {Enum.reverse(acc), data}
  end

  defp parse_array(data, nelems, parsers, acc) do
    case parse(data, parsers) do
      {values, rest} -> parse_array(rest, nelems - 1, parsers, [values|acc])
      :incomplete    -> :incomplete
    end
  end
end
