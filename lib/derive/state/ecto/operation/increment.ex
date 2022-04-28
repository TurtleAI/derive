defmodule Derive.State.Ecto.Operation.Increment do
  @moduledoc """
  Increment the value of a field by a delta

  You can think of it like:
  selected_record[field] += delta
  """

  defstruct [:selector, :field, :delta]
end

defimpl Derive.State.Ecto.DbOp, for: Derive.State.Ecto.Operation.Increment do
  import Derive.State.Ecto.Selector

  def to_multi(
        %Derive.State.Ecto.Operation.Increment{selector: selector, field: field, delta: delta},
        name
      ) do
    Ecto.Multi.update_all(Ecto.Multi.new(), name, selector_query(selector), inc: [{field, delta}])
  end
end
