import Config

config :logger, level: :info

config :derive, DeriveTestRepo,
  url: "postgres://postgres:postgres@localhost:5432/derive_test",
  pool: Ecto.Adapters.SQL.Sandbox
