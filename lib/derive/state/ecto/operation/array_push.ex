defmodule Derive.State.Ecto.Operation.ArrayPush do
  defstruct [:selector, :attr, :values, unique: false]
end

defimpl Derive.State.Ecto.DbOp, for: Derive.State.Ecto.Operation.ArrayPush do
  import Derive.State.Ecto.Util

  def to_multi(
        %Derive.State.Ecto.Operation.ArrayPush{
          selector: selector,
          attr: attr,
          values: values,
          unique: true
        },
        index
      ) do
    values
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {v, subindex}, acc ->
      array_push_uniq_query(acc, {index, subindex}, selector, attr, v)
    end)
  end
end
