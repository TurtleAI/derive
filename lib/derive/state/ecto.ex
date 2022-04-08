defmodule Derive.State.Ecto do
  @moduledoc """
  Operations to update derived state in Ecto
  """

  def commit(repo, operations) do
    multi = operations_to_multi(Ecto.Multi.new(), 1, operations)
    repo.transaction(multi)
  end

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
