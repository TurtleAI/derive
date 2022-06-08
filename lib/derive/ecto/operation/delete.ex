defmodule Derive.Ecto.Operation.Delete do
  @moduledoc """
  Delete a record identified by a selector

  You can think of it like:
  selected_record.delete()
  """

  defstruct [:selector]
end

defimpl Derive.Ecto.DbOp, for: Derive.Ecto.Operation.Delete do
  import Derive.Ecto.Selector

  def to_multi(%Derive.Ecto.Operation.Delete{selector: selector}, name) do
    Ecto.Multi.delete_all(Ecto.Multi.new(), name, selector_query(selector))
  end
end
