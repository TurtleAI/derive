defmodule Derive.Reducer.EventProcessor do
  alias Derive.State.{EventOp, MultiOp}
  alias Derive.Partition
  alias Derive.Timespan

  @type event :: Derive.EventLog.event()
  @type operation :: Derive.Reducer.operation()

  @type on_error :: :on_error | :halt

  @type option :: {:on_error, on_error()}

  @type event_handler :: (event() -> operation())
  @type commit_handler :: (MultiOp.t() -> MultiOp.t())

  @doc """
  Process events
  """
  @callback commit(MultiOp.t()) :: :ok

  @spec process_events([event()], MultiOp.t(), {event_handler(), commit_handler()}, [option()]) ::
          MultiOp.t()
  def process_events(
        events,
        %MultiOp{partition: %Partition{status: :error}} = multi,
        {_handle_event, commit},
        _opts
      ) do
    new_multi =
      Enum.reduce(events, multi, fn e, acc ->
        MultiOp.add(acc, EventOp.new(e, []))
      end)
      |> MultiOp.processed()

    commit.(new_multi)
  end

  def process_events(events, multi, {handle_event, commit}, opts) do
    case reduce_events(events, multi, handle_event, opts) do
      %MultiOp{status: :processed} = multi ->
        # we only commit a multi if it has successfully been processed
        commit.(multi)

      multi ->
        multi
    end
  end

  @doc """
  Execute the `handle_event` for all events and return a combined `Derive.State.MultiOp`
  that needs be committed for the state to update.

  For a reducer using Ecto-based state, this may be SQL queries represented by `Ecto.Multi`
  For an in-memory reducer, this might be some updates to an in-memory data structure.

  Depending on the on_error implementation of the reducer, an error may halt
  further processing or skip over the event.
  """
  @spec reduce_events(
          [event()],
          MultiOp.t(),
          Derive.Reducer.event_handler(),
          [option()]
        ) ::
          Derive.State.MultiOp.t()
  def reduce_events(events, multi, handle_event, opts) do
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
