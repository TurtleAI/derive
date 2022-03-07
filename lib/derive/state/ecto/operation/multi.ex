defimpl Derive.State.Ecto.DbOp, for: Ecto.Multi do
  def to_multi(%Ecto.Multi{} = multi, _index) do
    multi
  end
end
