defmodule MarcoPolo.GenericParser do
  @moduledoc """
  """

  @type ok_or_incomplete :: {term, binary} | :incomplete

  @type parser ::
    (binary -> ok_or_incomplete) | {:array, non_neg_integer, parser}

  @doc """
  """
  @spec parse(binary, [parser]) :: ok_or_incomplete
  def parse(data, parsers) when is_list(parsers) do
    parse(data, parsers, [])
  end

  defp parse(data, [parser|t], acc) do
    case apply_parser(parser, data) do
      {value, rest} -> parse(rest, t, [value|acc])
      :incomplete   -> :incomplete
    end
  end

  defp parse(data, [], acc) do
    {Enum.reverse(acc), data}
  end

  # Applies the given parser to data based on what `parser` is. For example, if
  # `parser` is a function with arity 1, applying it means calling it on `data`.
  defp apply_parser(parser, data)

  defp apply_parser(parser, data) when is_function(parser, 1) do
    parser.(data)
  end

  defp apply_parser({:array, nelems, parsers}, data) do
    parse_array(data, nelems, parsers, [])
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
