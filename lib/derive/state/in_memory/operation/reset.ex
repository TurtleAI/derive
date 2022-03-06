defmodule Derive.State.InMemory.Operation.Reset do
  defstruct []
end

defimpl Derive.State.InMemory.Reduce, for: Derive.State.InMemory.Operation.Reset do
  def reduce(_, _acc) do
    %{}
  end
end
