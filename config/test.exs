use Mix.Config

config :logger, level: :info

config :derive, DeriveEctoTest.Repo,
  url: "postgres://postgres:postgres@localhost:5432/derive_test",
  pool: Ecto.Adapters.SQL.Sandbox
