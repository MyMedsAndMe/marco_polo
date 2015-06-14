defmodule MarcoPolo.ConnectionTest do
  use ExUnit.Case

  alias MarcoPolo.Connection, as: C

  test "successfully connecting to the server without a token" do
    pid = connect(token?: false)
    assert [false] == C.operation(pid, :db_exist, ["nonexistent", "plocal"])
    assert [true]  == C.operation(pid, :db_exist, ["GratefulDeadConcerts", "plocal"])
  end

  test "successfully opening a db" do
    pid = connect(connection: {:db, "GratefulDeadConcerts", "plocal"})
    assert [size] = C.operation(pid, :db_size, [])
    assert is_integer(size)
  end

  defp connect(opts) do
    opts = Keyword.merge([user: user, password: password, connection: :server], opts)
    assert {:ok, pid} = C.start_link(opts)
    assert is_pid(pid)
    pid
  end

  defp user,     do: System.get_env("ORIENTDB_USER")
  defp password, do: System.get_env("ORIENTDB_PASS")
end
