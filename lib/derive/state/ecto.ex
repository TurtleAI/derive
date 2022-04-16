defmodule Derive.State.Ecto do
  @moduledoc """
  An Ecto-based implementation of state
  """

  @doc """
  Commit a list of operations to disk within a transaction.
  """
  def commit(repo, operations) do
    multi = operations_to_multi(Ecto.Multi.new(), 1, operations)
    repo.transaction(multi)
  end

  @doc """
  Completely erase all of the state to bring it back to the starting point.
  This involves dropping and recreating all the tables.
  """
  def reset_state(repo, models) do
    for model <- models do
      Ecto.Migration.Runner.run(repo, [], 0, model, :forward, :down, :down, [])
      Ecto.Migration.Runner.run(repo, [], 1, model, :forward, :up, :up, [])
    end
  end

  defp operations_to_multi(multi, _, []), do: multi

  defp operations_to_multi(multi, index, [op | rest]) do
    multi
    |> Ecto.Multi.append(Derive.State.Ecto.DbOp.to_multi(op, index))
    |> operations_to_multi(index + 1, rest)
  end
end
