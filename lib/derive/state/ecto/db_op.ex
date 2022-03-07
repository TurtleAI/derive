defprotocol Derive.State.Ecto.DbOp do
  @spec to_multi(t, integer()) :: Ecto.Multi.t()
  def to_multi(op, index)
end
