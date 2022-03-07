use Mix.Config

config :derive, Derive.Repo,
  url: "postgres://postgres:postgres@localhost:5432/derive_dev",
  pool: Ecto.Adapters.SQL.Sandbox
