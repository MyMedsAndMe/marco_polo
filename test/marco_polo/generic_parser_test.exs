defmodule MarcoPolo.GenericParserTest do
  use ExUnit.Case
  import MarcoPolo.GenericParser

  test "parse/1: single parser" do
    assert parse(<<1, 2>>, &byte_parser/1) == {1, <<2>>}
    assert parse(<<1 :: 16, 2>>, &int32_parser/1) == :incomplete
  end

  test "parse/1: simple list of parsers" do
    parsers = List.duplicate(&byte_parser/1, 2) ++ [&int32_parser/1]

    assert parse(<<1, 2, 42 :: 32, "rest">>, parsers) == {[1, 2, 42], "rest"}
    assert parse(<<>>, parsers) == :incomplete
    assert parse(<<1, 2, 42 :: 24>>, parsers) == :incomplete
  end

  test "parse/1: arrays of nested parsers" do
    parser = array_parser(&byte_parser/1, [&int32_parser/1, &byte_parser/1])
    assert parse(<<1, 12 :: 32, 0, "rest">>, [parser]) == {[[[12, 0]]], "rest"}
    assert parse(<<0, "rest">>, [parser]) == {[[]], "rest"}
    assert parse(<<2, 12 :: 32, 0, 12 :: 24>>, [parser]) == :incomplete

    parser = array_parser(&int32_parser/1, &byte_parser/1)
    assert parse(<<1 :: 32, 0, "rest">>, parser) == {[0], "rest"}
  end

  defp byte_parser(<<byte, rest :: binary>>), do: {byte, rest}
  defp byte_parser(<<>>), do: :incomplete

  defp int32_parser(<<i :: 32, rest :: binary>>), do: {i, rest}
  defp int32_parser(_), do: :incomplete
end
