defmodule Derive.Ecto.Operation.ArrayDelete do
  @moduledoc """
  For the record(s) identified by a given selector,
  remove the value from an array.

  You can think of it like:
  selected_record[field].delete(value)
  """

  defstruct [:selector, :field, :value]
end

defimpl Derive.Ecto.DbOp, for: Derive.Ecto.Operation.ArrayDelete do
  import Derive.Ecto.Selector

  def to_multi(
        %Derive.Ecto.Operation.ArrayDelete{selector: selector, field: field, value: value},
        name
      ) do
    Ecto.Multi.update_all(Ecto.Multi.new(), name, selector_query(selector), pull: [{field, value}])
  end
end
