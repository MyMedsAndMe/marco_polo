defmodule MarcoPolo.Mixfile do
  use Mix.Project

  @version "0.1.0"
  @supported_protocol 28

  def project do
    [app: :marco_polo,
     version: @version,
     elixir: "~> 1.0",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     aliases: ["test.all": "test --include scripting --include integration"],
     preferred_cli_env: ["test.all": :test],
     test_coverage: [tool: Coverex.Task],
     deps: deps]
  end

  def application do
    [applications: [:logger],
     env: [supported_protocol: @supported_protocol]]
  end

  defp deps do
    [{:decimal, "~> 1.1.0"},
     {:connection, "1.0.0-rc.1"},
     {:dialyze, "~> 0.2.0", only: :dev},
     {:coverex, "~> 1.4", only: :test},
     {:ex_doc, "~> 0.7", only: :docs}]
  end
end
