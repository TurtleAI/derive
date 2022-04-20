defmodule Derive.MixProject do
  use Mix.Project

  def project do
    [
      app: :derive,
      version: "0.1.0",
      elixir: "~> 1.8",
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
      {:ecto_sql, "~> 3.7.0"},
      {:postgrex, "~> 0.16.2"}
    ]
  end
end
