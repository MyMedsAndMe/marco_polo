defmodule MarcoPolo.GenericParserTest do
  use ExUnit.Case

  import MarcoPolo.GenericParser

  test "parse/1: simple list of parsers" do
    parsers = List.duplicate(&byte_parser/1, 2) ++ [&int32_parser/1]

    assert parse(<<1, 2, 42 :: 32, "rest">>, parsers) == {[1, 2, 42], "rest"}
    assert parse(<<>>, parsers) == :incomplete
    assert parse(<<1, 2, 42 :: 24>>, parsers) == :incomplete
  end

  test "parse/1: arrays of nested parsers" do
    parser = {:array, 2, [(&int32_parser/1)|List.duplicate((&byte_parser/1), 2)]}

    assert parse(<<0 :: 32, 1, 2, 0 :: 32, 3, 4, 5>>, [parser]) ==
           {[[[0, 1, 2], [0, 3, 4]]], <<5>>}
    assert parse(<<1, 2, 3>>, [parser]) == :incomplete
  end

  defp byte_parser(<<byte, rest :: binary>>), do: {byte, rest}
  defp byte_parser(<<>>), do: :incomplete

  defp int32_parser(<<i :: 32, rest :: binary>>), do: {i, rest}
  defp int32_parser(_), do: :incomplete
end
