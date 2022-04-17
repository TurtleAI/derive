defmodule Derive.Util do
  def handle_events(events, reducer, partition) do
    event_operations =
      events
      |> Enum.map(fn e ->
        {e, List.wrap(reducer.handle_event(e))}
      end)
      |> Enum.reject(fn {_e, ops} -> ops == [] end)

    Derive.State.MultiOp.new(partition, event_operations)
  end
end
