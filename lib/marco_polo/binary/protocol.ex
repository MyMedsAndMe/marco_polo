import MarcoPolo.Binary.Helpers

defprotocol MarcoPolo.Binary.Protocol do
  def encode(el, type \\ nil)
end

defimpl MarcoPolo.Binary.Protocol, for: Atom do
  def encode(true, _), do: <<1>>
  def encode(false, _), do: <<0>>
  def encode(_, _), do: raise(ArgumentError, "only atoms true and false can be encoded")
end

defimpl MarcoPolo.Binary.Protocol, for: Integer do
  def encode(i, :short), do: <<i :: short>>
  def encode(i, :int),   do: <<i :: int>>
  def encode(i, :long),  do: <<i :: long>>
end

defimpl MarcoPolo.Binary.Protocol, for: BitString do
  def encode(<<str :: binary>>, _) do
    MarcoPolo.Binary.Protocol.encode(byte_size(str), :int) <> str
  end
end
