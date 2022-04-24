defmodule Derive.Reducer.Util do
  alias Derive.State.MultiOp

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

  defp do_reduce([], multi, _handle_event, _),
    do: MultiOp.processed(multi)

  defp do_reduce([event | rest], multi, handle_event, on_error) do
    resp =
      try do
        {:ok, handle_event.(event)}
      rescue
        error ->
          {:error, error}
      end

    case resp do
      {:ok, ops} ->
        do_reduce(rest, MultiOp.add(multi, event, ops), handle_event, on_error)

      {:error, error} ->
        case on_error do
          :skip ->
            do_reduce(rest, MultiOp.add_error(multi, event, error), handle_event, on_error)

          :halt ->
            MultiOp.failed_on_event(multi, event, error)
        end
    end
  end
end
