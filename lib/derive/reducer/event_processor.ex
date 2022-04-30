defmodule Derive.Reducer.EventProcessor do
  alias Derive.State.MultiOp
  alias Derive.State.EventOp
  alias Derive.Timespan

  @doc """
  Execute the `handle_event` for all events and return a combined `Derive.State.MultiOp`
  that needs be committed for the state to update.

  For a reducer using Ecto-based state, this may be SQL queries represented by `Ecto.Multi`
  For an in-memory reducer, this might be some updates to an in-memory data structure.

  Depending on the on_error implementation of the reducer, an error may halt
  further processing or skip over the event.
  """
  @spec reduce_events(
          [Derive.EventLog.event()],
          Derive.State.MultiOp.t(),
          Derive.Reducer.event_handler()
        ) ::
          Derive.State.MultiOp.t()
  def reduce_events(events, multi, handle_event, opts \\ []) do
    on_error = Keyword.get(opts, :on_error, :halt)
    do_reduce(events, multi, handle_event, on_error)
  end

  defp do_reduce([], %MultiOp{status: :processing} = multi, _handle_event, _),
    do: MultiOp.processed(multi)

  defp do_reduce([], %MultiOp{} = multi, _handle_event, _),
    do: multi

  defp do_reduce([event | rest], multi, handle_event, on_error) do
    timespan = Timespan.start()

    resp =
      try do
        ops = handle_event.(event)
        {:ok, EventOp.new(event, ops, Timespan.stop(timespan))}
      rescue
        error ->
          {:error, EventOp.error(event, error, Timespan.stop(timespan))}
      end

    case resp do
      {:ok, event_op} ->
        do_reduce(rest, MultiOp.add(multi, event_op), handle_event, on_error)

      {:error, event_op} ->
        case on_error do
          :skip ->
            do_reduce(rest, MultiOp.add(multi, event_op), handle_event, on_error)

          :halt ->
            MultiOp.failed_on_event(multi, event_op)
        end
    end
  end
end
