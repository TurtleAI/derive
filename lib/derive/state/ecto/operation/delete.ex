defmodule Derive.State.Ecto.Operation.Delete do
  defstruct [:selector]
end

defimpl Derive.State.Ecto.DbOp, for: Derive.State.Ecto.Operation.Delete do
  import Derive.State.Ecto.Util

  def to_multi(%Derive.State.Ecto.Operation.Delete{selector: selector}, index) do
    Ecto.Multi.delete_all(Ecto.Multi.new(), index, selector_query(selector))
  end
end
