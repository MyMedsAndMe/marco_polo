defmodule MarcoPolo.Mixfile do
  use Mix.Project

  @client_name "MarcoPolo (Elixir driver)"
  @binary_protocol_version 33
  @version "0.2.1"

  def project do
    [app: :marco_polo,
     version: @version,
     elixir: "~> 1.0",

     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,

     # Testing
     aliases: ["test.all": &test_all/1],
     preferred_cli_env: ["test.all": :test],
     test_coverage: [tool: Coverex.Task],

     # Hex
     package: package,
     description: description,

     # Docs
     name: "MarcoPolo",
     source_url: "https://github.com/MyMedsAndMe/marco_polo",

     deps: deps]
  end

  def application do
    [applications: [:logger, :connection, :ssl, :hackney, :poison],
     env: [client_name: @client_name,
           version: @version,
           binary_protocol_version: @binary_protocol_version]]
  end

  defp package do
    [maintainers: ["Andrea Leopardi"],
     licenses: ["Apache"],
     links: %{"GitHub" => "https://github.com/MyMedsAndMe/marco_polo"}]
  end

  defp description do
    """
    Binary driver for the OrientDB database.
    """
  end

  defp deps do
    [{:decimal, "~> 1.1.0"},
     {:connection, "~> 1.0.0"},
     {:hackney, "~> 1.6.0"},
     {:poison, "~> 2.0.0"},
     {:dialyze, "~> 0.2.0", only: :dev},
     {:earmark, ">= 0.0.0", only: :docs},
     {:ex_doc, ">= 0.0.0", only: :docs}]
  end

  defp test_all(args) do
    args = if IO.ANSI.enabled?, do: ["--color"|args], else: ["--no-color"|args]
    args = ~w(--include scripting) ++ args

    vsn = System.get_env("ORIENTDB_VERSION")
    if is_nil(vsn) or Version.compare(vsn, "2.1.0") in [:eq, :gt] do
      args = ~w(--include live_query) ++ args
    end

    args = ~w(--include ssl) ++ args

    Mix.Task.run "test", args
  end
end
