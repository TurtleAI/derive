defmodule Derive.MixProject do
  use Mix.Project

  def project do
    [
      app: :derive,
      version: "0.1.0",
      elixir: "~> 1.14.4",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:ecto_sql, "~> 3.10.1"},
      {:jason, "~> 1.4"},
      {:postgrex, "~> 0.17.1"}
    ]
  end
end
