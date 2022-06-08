defmodule Derive.Ecto.Operation.Increment do
  @moduledoc """
  Increment the value of a field by a delta

  You can think of it like:
  selected_record[field] += delta
  """

  defstruct [:selector, :field, :delta]
end

defimpl Derive.Ecto.DbOp, for: Derive.Ecto.Operation.Increment do
  import Derive.Ecto.Selector

  def to_multi(
        %Derive.Ecto.Operation.Increment{selector: selector, field: field, delta: delta},
        name
      ) do
    Ecto.Multi.update_all(Ecto.Multi.new(), name, selector_query(selector), inc: [{field, delta}])
  end
end
