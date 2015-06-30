defmodule MarcoPolo.Date do
  defstruct year: 0, month: 1, day: 1
end

defmodule MarcoPolo.DateTime do
  defstruct year: 0, month: 1, day: 1,
            hour: 0, min: 0, sec: 0, msec: 0
end
