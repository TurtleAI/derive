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

      multis =
        changes
        |> List.flatten()
        |> Enum.with_index(1)
        |> Enum.map(fn {op, index} -> to_multi(op, index) end)

      combined_multi = Enum.reduce(multis, Ecto.Multi.new(), &Ecto.Multi.append(&2, &1))
      repo.transaction(combined_multi)

      {:ok, nil}
    end)
  end
end
