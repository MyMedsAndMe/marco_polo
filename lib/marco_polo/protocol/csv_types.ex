defmodule MarcoPolo.Protocol.CSVTypes do
  @moduledoc false

  # Some types are missing:
  #
  #   * link lists/sets/maps
  #   * embedded
  #   * embedded lists/sets/maps
  #
  # Most likely we won't need to implement CSV decoding for those types because
  # right now we're only implementing CSV decoding because that's how
  # `REQUEST_COMMAND` sometimes responds (specifically, when it returns what it
  # calls a 'serialized result'), and the types it responds are simple. This
  # module should go away as soon as OrientDB fixes this behaviour of encoding
  # some things (very few) still using the CSV protocol.

  @digits '0123456789'

  @doc ~S"""
  Decode a comma-separated list of values encoded as specified by OrientDB's
  [CSV serialization
  protocol](http://orientdb.com/docs/last/Record-CSV-Serialization.html).

  ## Examples

      iex> MarcoPolo.Protocol.CSVTypes.decode("3.14f")
      3.14
      iex> MarcoPolo.Protocol.CSVTypes.decode("\"foo\"")
      "foo"

  """
  @spec decode(binary) :: nil | binary | integer | float | boolean | MarcoPolo.RID.t
  def decode(data)

  # Strings
  def decode("\"" <> rest) do
    [str, ""] = :binary.split(rest, "\"")
    str
  end

  # Binaries
  def decode("_" <> rest) do
    [b64, ""] = :binary.split(rest, "_")
    Base.decode64!(b64)
  end

  # Floats and ints
  def decode(<<?-, char, _ :: binary>> = data) when char in @digits do
    decode_numeric(data)
  end

  def decode(<<char, _ :: binary>> = data) when char in @digits do
    decode_numeric(data)
  end

  # Booleans
  def decode("true"),  do: true
  def decode("false"), do: false

  # RIDs
  def decode("#" <> rest) do
    [cluster, position] = String.split(rest, ":")
    %MarcoPolo.RID{cluster_id: String.to_integer(cluster),
                   position: String.to_integer(position)}
  end

  # nil
  def decode(""), do: nil

  defp decode_numeric(data) do
    regex = ~r/^(?<digits>-?\d+(\.\d+)?)(?<mod>[a-z])?$/

    case Regex.named_captures(regex, data) do
      %{"mod" => mod, "digits" => digits} when mod in ["s", "l", ""] ->
        decode_int(digits)
      %{"mod" => mod, "digits" => digits} when mod in ["f", "d"] ->
        decode_float(digits)
      %{"mod" => "c", "digits" => digits} ->
        decode_decimal(digits)
      %{"mod" => "a", "digits" => digits} ->
        decode_date(digits)
      %{"mod" => "t", "digits" => digits} ->
        decode_datetime(digits)
    end
  end

  defp decode_float(digits) do
    String.to_float(digits)
  end

  defp decode_int(digits) do
    String.to_integer(digits)
  end

  defp decode_decimal(data) do
    Decimal.new(data)
  end

  defp decode_date(digits) do
    secs_from_epoch = digits |> String.to_integer |> div(1000)
    {{yy, mm, dd}, _} = unix_timestamp_to_datetime(secs_from_epoch)
    %MarcoPolo.Date{year: yy, month: mm, day: dd}
  end

  defp decode_datetime(digits) do
    digits = String.to_integer(digits)
    secs_from_epoch = div(digits, 1000)
    msec = rem(digits, 1000)

    {{yy, mm, dd}, {h, m, s}} = unix_timestamp_to_datetime(secs_from_epoch)
    %MarcoPolo.DateTime{year: yy, month: mm, day: dd,
                        hour: h, min: m, sec: s, msec: msec}
  end

  defp unix_timestamp_to_datetime(seconds) do
    epoch = :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
    :calendar.gregorian_seconds_to_datetime(epoch + seconds)
  end
end
