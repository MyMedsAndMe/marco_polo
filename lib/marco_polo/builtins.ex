defmodule MarcoPolo.Date do
  @doc """
  Struct that expresses an OrientDB date.

  Fields:

    * `:year` - defaults to 0
    * `:month` - defaults to 1
    * `:day` - defaults to 1

  """

  @type t :: %__MODULE__{
    year: non_neg_integer,
    month: 1..12,
    day: 1..31,
  }

  defstruct year: 0, month: 1, day: 1
end

defmodule MarcoPolo.DateTime do
  @doc """
  Struct that expresses an OrientDB datetime.

  Fields:

    * `:year` - defaults to 0
    * `:month` - defaults to 1
    * `:day` - defaults to 1
    * `:hour` - defaults to 0
    * `:min` - defaults to 0
    * `:sec` - defaults to 0
    * `:msec` - defaults to 0

  """

  @type t :: %__MODULE__{
    year: non_neg_integer,
    month: 1..12,
    day: 1..31,
    hour: 0..23,
    min: 0..59,
    sec: 0..59,
    msec: 0..999,
  }

  defstruct year: 0, month: 1, day: 1,
            hour: 0, min: 0, sec: 0, msec: 0
end
