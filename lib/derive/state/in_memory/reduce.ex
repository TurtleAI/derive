defprotocol Derive.State.InMemory.Reduce do
  @spec reduce(t, any()) :: any()
  def reduce(t, acc)
end
