defmodule Derive.Util do
  alias Derive.State.MultiOp

  @doc """
  Execute the `handle_event` and return the collected operations
  for all the executed handlers.

  The operations that come from `Derive.State.MultiOp` have not yet been committed.
  """
  @spec process_events(
          [Derive.EventLog.event()],
          Derive.Reducer.t(),
          Derive.Reducer.partition()
        ) ::
          Derive.State.MultiOp.t()
  def process_events(events, reducer, partition) do
    MultiOp.new(partition)
    |> do_process(events, reducer)
  end

  defp do_process(multi, [], _reducer), do: multi

  defp do_process(multi, [event | rest], reducer) do
    # current overall behavior is to skip failed events
    # @TODO: replace with more explicit error handling which may vary per use case
    # For example:
    # - Skip over the failed event
    # - Stop processing all future events

    resp =
      try do
        {:ok, reducer.handle_event(event)}
      rescue
        error ->
          {:error, error}
      end

    case resp do
      {:ok, ops} ->
        MultiOp.add(multi, event, ops)
        |> do_process(rest, reducer)

      {:error, error} ->
        MultiOp.error(multi, event, error)
        |> do_process(rest, reducer)
    end
  end
end
