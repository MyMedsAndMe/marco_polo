defmodule MarcoPolo.RESTTest do
  use ExUnit.Case

  @moduletag :integration
  @moduletag :rest_api

  alias MarcoPolo.REST

  @opts [
    user: TestHelpers.user(),
    password: TestHelpers.password(),
  ]

  test "importing a valid JSON into an existing db" do
    db_path = fixture_path("exported_db.json")
    assert :ok = REST.import("MarcoPoloImportDest", db_path, @opts)
  end

  test "importing a valid JSON into a non-existing db" do
    db_path = fixture_path("exported_db.json")
    assert {:error, err} = REST.import("Nonexistent", db_path, @opts)
    assert err =~ "error while importing (status 401)"
  end

  test "importing an invalid JSON into an existing db" do
    db_path = fixture_path("invalid_json.json")
    assert :ok = MarcoPolo.REST.import("MarcoPoloImportDest", db_path, @opts)
  end

  test "connecting to a non-running server" do
    db_path = fixture_path("invalid_json.json")
    opts = @opts ++ [host: "nonexistent"]
    assert {:error, :nxdomain} = REST.import("MarcoPoloImportDest", db_path, opts)
  end

  defp fixture_path(path) do
    "../fixtures"
    |> Path.expand(__DIR__)
    |> Path.join(path)
  end
end
