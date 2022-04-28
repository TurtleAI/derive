defmodule Derive.State.Ecto.Operation.Increment do
  defstruct [:selector, :attr, :delta]
end

defimpl Derive.State.Ecto.DbOp, for: Derive.State.Ecto.Operation.Increment do
  import Derive.State.Ecto.Selector

  def to_multi(
        %Derive.State.Ecto.Operation.Increment{selector: selector, attr: attr, delta: delta},
        index
      ) do
    Ecto.Multi.update_all(Ecto.Multi.new(), index, selector_query(selector), inc: [{attr, delta}])
  end
end
