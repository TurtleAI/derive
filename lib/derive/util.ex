defmodule Derive.Util do
  alias Derive.State.MultiOp

  @doc """
  Execute the `handle_event` for all events and return a combined operation
  that needs be committed for the state to update.

  For a reducer using Ecto-based state, this may be SQL queries represented by `Ecto.Multi`
  For an in-memory reducer, this might be some updates to an in-memory data structure.

  Depending on the on_error implementation of the reducer, an error may halt
  further processing or skip over the event.
  """
  @spec process_events(
          [Derive.EventLog.event()],
          Derive.Reducer.t(),
          Derive.Partition.t()
        ) ::
          Derive.State.MultiOp.t()
  def process_events(events, reducer, partition) do
    MultiOp.new(partition)
    |> do_process(events, reducer, reducer.on_error())
  end

  defp do_process(multi, [], _reducer, _),
    do: MultiOp.processed(multi)

  defp do_process(multi, [event | rest], reducer, on_error) do
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
        |> do_process(rest, reducer, on_error)

      {:error, error} ->
        case on_error do
          :skip ->
            MultiOp.add_error(multi, event, error)
            |> do_process(rest, reducer, on_error)

          :halt ->
            MultiOp.failed_on_event(multi, event, error)
        end
    end
  end
end
