defmodule MarcoPolo.Mixfile do
  use Mix.Project

  @version "0.0.1-dev"
  @supported_protocol 28

  def project do
    [app: :marco_polo,
     version: @version,
     elixir: "~> 1.0",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps]
  end

  def application do
    [applications: [:logger],
     env: [supported_protocol: @supported_protocol]]
  end

  defp deps do
    [{:decimal, "~> 1.1.0"},
     {:connection, github: "fishcakez/connection"},
     {:small_ints, github: "whatyouhide/small_ints"},
     {:ex_doc, "~> 0.7", only: :docs}]
  end
end
