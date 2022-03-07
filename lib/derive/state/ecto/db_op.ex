defprotocol Derive.State.Ecto.DbOp do
  @spec to_multi(any(), number()) :: Ecto.Multi.t()
  def to_multi(op, index)
end
