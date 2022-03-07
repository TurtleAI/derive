defimpl Derive.State.Ecto.DbOp, for: Ecto.Query do
  def to_multi(%Ecto.Query{} = query, index) do
    Ecto.Multi.update_all(Ecto.Multi.new(), index, query, [])
  end
end
