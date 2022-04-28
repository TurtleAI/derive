defmodule Derive.State.Ecto.Operation.ArrayDelete do
  defstruct [:selector, :attr, :value]
end

defimpl Derive.State.Ecto.DbOp, for: Derive.State.Ecto.Operation.ArrayDelete do
  import Derive.State.Ecto.Selector

  def to_multi(
        %Derive.State.Ecto.Operation.ArrayDelete{selector: selector, attr: attr, value: value},
        index
      ) do
    Ecto.Multi.update_all(Ecto.Multi.new(), index, selector_query(selector), pull: [{attr, value}])
  end
end
