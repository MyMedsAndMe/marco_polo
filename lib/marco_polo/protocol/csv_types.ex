defmodule MarcoPolo.Protocol.CSVTypes do
  @moduledoc false

  @digits '0123456789'

  @doc """
  Decode a comma-separated list of values encoded as specified by OrientDB's
  [CSV serialization
  protocol](http://orientdb.com/docs/last/Record-CSV-Serialization.html).

  ## Examples

      iex> MarcoPolo.Protocol.CSVTypes.decode("true,1,1.45,\"foo\")
      [true, 1, 1.45, "foo"]

  """
  @spec decode(binary) :: [binary | integer | float | boolean | nil]
  def decode(data) do
    data
    |> :binary.split(",", [:global])
    |> Enum.map(&decode_field/1)
  end

  # Strings
  defp decode_field("\"" <> rest) do
    [str, ""] = :binary.split(rest, "\"")
    str
  end

  # Binaries
  defp decode_field("_" <> rest) do
    [b64, ""] = :binary.split(rest, "_")
    Base.decode64!(b64)
  end

  # Floats and ints
  defp decode_field(<<char, _ :: binary>> = data) when char in @digits do
    # This regex tests that `data` starts with some numbers and then a dot,
    # which means a float. Otherwise, parse an integer.
    parser = if Regex.match?(~r/^\d+\./, data), do: :to_float, else: :to_integer
    apply(String, parser, [data])
  end

  # Booleans
  defp decode_field("true"),  do: true
  defp decode_field("false"), do: false

  # nil
  defp decode_field(""), do: nil
end
