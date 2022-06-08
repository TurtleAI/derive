defimpl Derive.Ecto.DbOp, for: Ecto.Query do
  @moduledoc """
  Pass-through implementation for an `Ecto.Query` that issues an
  `&Ecto.Multi.update_all/4` query.`
  """

  def to_multi(%Ecto.Query{} = query, index) do
    Ecto.Multi.update_all(Ecto.Multi.new(), index, query, [])
  end
end
