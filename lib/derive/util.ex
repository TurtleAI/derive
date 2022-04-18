defmodule Derive.Util do
  def handle_events(events, reducer, partition) do
    event_operations =
      Enum.map(events, fn e ->
        # current overall behavior is to skip failed events
        # @TODO: replace with more explicit error handling which may vary per use case
        # For example:
        # - Skip over the failed event
        # - Stop processing all future events
        resp =
          try do
            {:ok, reducer.handle_event(e)}
          rescue
            error ->
              {:error, error}
          end

        case resp do
          {:ok, ops} ->
            {e, {:ok, List.wrap(ops)}}

          {:error, error} ->
            {e, {:error, error}}
        end
      end)
      |> Enum.reject(fn
        {:ok, {_e, []}} ->
          true

        _ ->
          false
      end)

    Derive.State.MultiOp.new(partition, event_operations)
  end
end
