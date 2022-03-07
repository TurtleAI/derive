defmodule Derive.State.Ecto.Model do
  defmacro __using__(_) do
    quote do
      use Ecto.Schema
      use Ecto.Migration
    end
  end
end
