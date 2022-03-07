use Mix.Config

config :derive, Derive.Repo,
  url: "postgres://postgres:postgres@localhost:5432/derive_test",
  pool: Ecto.Adapters.SQL.Sandbox
