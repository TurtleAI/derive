defmodule Derive.Repo do
  # Repo only used for testing purposes
  use Ecto.Repo, otp_app: :derive, adapter: Ecto.Adapters.Postgres
end
