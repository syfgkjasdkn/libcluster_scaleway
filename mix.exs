defmodule LibclusterScaleway.MixProject do
  use Mix.Project

  def project do
    [
      app: :libcluster_scaleway,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.1"},
      {:libcluster, "~> 3.0"},
      # TODO remove
      {:linkex, "~> 0.1"}
    ]
  end
end
