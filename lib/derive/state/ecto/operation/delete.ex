defmodule Derive.State.Ecto.Operation.Delete do
  @moduledoc """
  Delete a record identified by a selector

  You can think of it like:
  selected_record.delete()
  """

  defstruct [:selector]
end

defimpl Derive.State.Ecto.DbOp, for: Derive.State.Ecto.Operation.Delete do
  import Derive.State.Ecto.Selector

  def to_multi(%Derive.State.Ecto.Operation.Delete{selector: selector}, name) do
    Ecto.Multi.delete_all(Ecto.Multi.new(), name, selector_query(selector))
  end
end
