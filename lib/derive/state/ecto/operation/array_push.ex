defmodule Derive.State.Ecto.Operation.ArrayPush do
  @moduledoc """
  For the record(s) identified by a given selector,
  add a value to an array.

  You can think of it like:
  selected_record[field].add(value)
  """

  defstruct [:selector, :field, :values, unique: false]
end

defimpl Derive.State.Ecto.DbOp, for: Derive.State.Ecto.Operation.ArrayPush do
  import Derive.State.Ecto.Selector

  def to_multi(
        %Derive.State.Ecto.Operation.ArrayPush{
          selector: selector,
          field: field,
          values: values,
          unique: true
        },
        name
      ) do
    values
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {v, index}, acc ->
      array_push_uniq_query(acc, {name, index}, selector, field, v)
    end)
  end

  defp array_push_uniq_query(%Ecto.Multi{} = multi, name, selector, field, value) do
    import Ecto.Query
    query = from(rec in selector_query(selector), where: ^value not in field(rec, ^field))
    Ecto.Multi.update_all(multi, name, query, push: [{field, value}])
  end
end
