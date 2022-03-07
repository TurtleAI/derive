defprotocol Derive.State.Ecto.DbOp do
  @spec to_multi(op :: any(), index :: number()) :: Ecto.Multi.t()
  def to_multi(op, index)
end
