defmodule Derive.Util do
  def process_events(events, reducer: reducer, partition: partition, version: version) do
    event_operations =
      events
      |> Enum.map(fn e ->
        {e, List.wrap(reducer.handle_event(e))}
      end)
      |> Enum.reject(fn {_e, ops} -> ops == [] end)

    multi_op = Derive.State.MultiOp.new(partition, event_operations)

    reducer.commit_operations(multi_op)

    new_version = compute_new_version(version, events)

    {:ok, new_version}
  end

  defp compute_new_version(version, []),
    do: version

  defp compute_new_version(_version, events) do
    events |> Enum.map(fn %{id: id} -> id end) |> Enum.max()
  end
end
