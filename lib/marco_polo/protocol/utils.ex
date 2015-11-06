defmodule MarcoPolo.Protocol.Utils do
  @moduledoc false

  @doc """
  Performs a reduce operation `n` times, producing a list and an accumulator.

  `start_acc` is the initial value of the accumulator. `fun` takes the
  accumulator and has to return a two-element with the element to build the
  mapped list as the first element and the new value for the accumulator as the
  second element.
  """
  @spec reduce_n_times(non_neg_integer, acc, (acc -> {term, acc}))
    :: {[term], acc} when acc: term
  def reduce_n_times(n, start_acc, fun) do
    Enum.map_reduce List.duplicate(nil, n), start_acc, fn(_, acc) ->
      fun.(acc)
    end
  end
end
