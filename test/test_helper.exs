defmodule DeriveTestRepo do
  use Ecto.Repo, otp_app: :derive, adapter: Ecto.Adapters.Postgres
end

ExUnit.start()
