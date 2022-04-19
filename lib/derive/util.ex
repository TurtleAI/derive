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
          Derive.Partition.t()
        ) ::
          Derive.State.MultiOp.t()
  def process_events(events, reducer, partition, opts \\ []) do
    on_error = Keyword.get(opts, :on_error, :skip)

    MultiOp.new(partition)
    |> do_process(events, reducer, on_error)
  end

  defp do_process(multi, [], _reducer, _),
    do: MultiOp.processed(multi)

  defp do_process(multi, [event | rest], reducer, :skip) do
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
        |> do_process(rest, reducer, :skip)

      {:error, error} ->
        MultiOp.add_error(multi, event, error)
        |> do_process(rest, reducer, :skip)
    end
  end
end
