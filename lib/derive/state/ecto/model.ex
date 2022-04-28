defmodule Derive.State.Ecto.Model do
  @moduledoc """
  A database-backed model that's essentially an `Ecto.Schema`
  but also bundled with up/down functions create and drop the underlying tables.

  The schema and migrations are co-located to make updating the schema
  and the underlying fields easier.

  Calling `Derive.rebuild/2` does a full rebuild of the state including dropping
  and recreating all the tables.
  """

  @type t :: module()

  defmacro __using__(_) do
    quote do
      use Ecto.Schema
      use Ecto.Migration
    end
  end
end
