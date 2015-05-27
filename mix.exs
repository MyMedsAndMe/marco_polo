defmodule MarcoPolo.Mixfile do
  use Mix.Project

  def project do
    [app: :marco_polo,
     version: "0.0.1",
     elixir: "~> 1.0",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps,

     build_per_environment: false,

     # Testing
     test_paths: if(Mix.env == :integration, do: ["integration_test"], else: ["test"]),
     aliases: ["test.all": &test_all/1],
     preferred_cli_env: ["test.all": :test]]
  end

  def application do
    [applications: [:logger]]
  end

  defp deps do
    []
  end

  # This beauty is taken almost verbatim from the mix.exs file of the Ecto
  # project (https://github.com/elixir-lang/ecto/blob/v0.11.3/mix.exs).
  defp test_all(args) do
    args = if IO.ANSI.enabled?, do: ["--color"|args], else: ["--no-color"|args]
    Mix.Task.run "test", args

    {_, res} = System.cmd("mix", ["test"|args],
                          into: IO.binstream(:stdio, :line),
                          env: [{"MIX_ENV", "integration"}])

    if res > 0 do
      System.at_exit(fn _ -> exit({:shutdown, 1}) end)
    end
  end
end
