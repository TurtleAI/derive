defprotocol Derive.State.InMemory.Reduce do
  @spec reduce(t, term()) :: term()
  def reduce(t, acc)
end
