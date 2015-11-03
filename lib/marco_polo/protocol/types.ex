defmodule MarcoPolo.Protocol.Types do
  @moduledoc false

  alias MarcoPolo.Document
  alias MarcoPolo.BinaryRecord
  alias MarcoPolo.Protocol.RecordSerialization

  import MarcoPolo.Protocol.BinaryHelpers

  @type encodable_term ::
    boolean
    | nil
    | binary
    | integer
    | iolist
    | {:short, integer}
    | {:int, integer}
    | {:long, integer}
    | {:raw, binary}
    | Document.t
    | BinaryRecord.t

  @doc """
  Encodes a given term according to the binary protocol.

  The type of `term` is usually inferred by its value but in some cases it can
  be specified by using a tagged tuple. For example, to force encodng of an
  integer as an OrientDB short, you can pass `{:short, n}`.
  """
  # Made public for testing.
  @spec encode(encodable_term) :: iodata
  def encode(term)

  # Booleans.
  def encode(true),  do: <<1>>
  def encode(false), do: <<0>>

  # nil.
  def encode(nil), do: encode({:int, -1})

  # Strings and bytes.
  def encode(str) when is_binary(str), do: encode({:int, byte_size(str)}) <> str

  # Encoding an Elixir integer defaults to encoding an OrientDB int (4 bytes).
  def encode(i) when is_integer(i), do: encode({:int, i})

  # Typed integers (short, int and long) have to be tagged.
  def encode({:short, i}), do: <<i :: short>>
  def encode({:int, i}),   do: <<i :: int>>
  def encode({:long, i}),  do: <<i :: long>>

  # A list is assumed to be iodata and is converted to binary before being serialized.
  def encode(data) when is_list(data), do: [encode(IO.iodata_length(data)), data]

  # Raw bytes (that have no leading length, just the bytes).
  def encode({:raw, bytes}) when is_binary(bytes) or is_list(bytes), do: bytes

  # An entire document.
  def encode(%Document{} = record), do: encode(RecordSerialization.encode(record))

  # A binary record (BLOB).
  def encode(%BinaryRecord{contents: bytes}), do: encode(bytes)

  @doc """
  Encdes a list of terms.
  """
  @spec encode_list([MarcoPolo.Protocol.Types.encodable_term]) :: iodata
  def encode_list(list) when is_list(list) do
    Enum.map(list, &encode/1)
  end

  @doc """
  Decodes an instance of `type` from `data`.

  Returns a `{value, rest}` tuple or the `:incomplete` atom if `data` doesn't
  contain a full instance of `type`.
  """
  @spec decode(binary, atom) :: {term, binary} | :incomplete
  def decode(data, type)

  def decode(<<-1 :: int, rest :: binary>>, type) when type in [:string, :bytes] do
    {nil, rest}
  end

  def decode(<<length :: int, data :: binary>>, type) when type in [:string, :bytes] do
    case data do
      <<parsed :: bytes-size(length), rest :: binary>> -> {parsed, rest}
      _                                                -> :incomplete
    end
  end

  def decode(<<byte, rest :: binary>>, :byte), do: {byte, rest}

  def decode(<<i :: short, rest :: binary>>, :short), do: {i, rest}
  def decode(<<i :: int, rest :: binary>>, :int),     do: {i, rest}
  def decode(<<i :: long, rest :: binary>>, :long),   do: {i, rest}

  def decode(_data, _type) do
    :incomplete
  end
end
