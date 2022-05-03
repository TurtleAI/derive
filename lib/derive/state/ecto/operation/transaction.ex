defmodule Derive.State.Ecto.Operation.Transaction do
  @moduledoc """
  Run a transaction given by the function.
  The function has access to a repo and can perform any necessary db changes within the transaction.
  """
  defstruct [:fun]
end

defimpl Derive.State.Ecto.DbOp, for: Derive.State.Ecto.Operation.Transaction do
  def to_multi(%Derive.State.Ecto.Operation.Transaction{fun: fun}, index) when is_function(fun) do
    Ecto.Multi.run(Ecto.Multi.new(), index, fn repo, _ ->
      changes = List.wrap(fun.(repo))

      produced_multi = operations_to_multi(Ecto.Multi.new(), 0, List.flatten(changes))
      repo.transaction(produced_multi)

      {:ok, nil}
    end)
  end

  defp operations_to_multi(multi, _, []), do: multi

  defp operations_to_multi(multi, index, [op | rest]) do
    multi
    |> Ecto.Multi.append(Derive.State.Ecto.DbOp.to_multi(op, index))
    |> operations_to_multi(index + 1, rest)
  end
end
