defmodule Derive.Reducer.EventProcessor do
  alias Derive.State.{EventOp, MultiOp}
  alias Derive.{Partition, Timespan}

  require Logger

  @type event :: Derive.EventLog.event()
  @type cursor :: Derive.EventLog.cursor()
  @type operation :: Derive.Reducer.operation()

  @type on_error :: :halt

  @type option :: {:on_error, on_error()}

  @type event_handler :: (event() -> operation())
  @type cursor_handler :: (event() -> cursor())
  @type commit_handler :: (MultiOp.t() -> MultiOp.t())

  @doc """
  Process events
  """
  @callback commit(MultiOp.t()) :: :ok

  @spec process_events(
          [event()],
          MultiOp.t(),
          {event_handler(), cursor_handler(), commit_handler()},
          [option()]
        ) ::
          MultiOp.t()
  def process_events(events, multi, {handle_event, get_cursor, commit}, opts) do
    case reduce_events(events, multi, {handle_event, get_cursor}, opts) do
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
          {event_handler(), cursor_handler()},
          [option()]
        ) ::
          Derive.State.MultiOp.t()
  def reduce_events(events, multi, {handle_event, get_cursor}, opts) do
    on_error = Keyword.get(opts, :on_error, :halt)
    do_reduce(events, multi, {handle_event, get_cursor}, on_error)
  end

  defp do_reduce([], %MultiOp{status: :processing} = multi, _handlers, _),
    do: MultiOp.processed(multi)

  defp do_reduce([], %MultiOp{} = multi, _handlers, _),
    do: multi

  defp do_reduce(
         [event | rest],
         %MultiOp{partition: %Partition{status: status, cursor: cursor}} = multi,
         {handle_event, get_cursor},
         on_error
       ) do
    timespan = Timespan.start()
    event_cursor = get_cursor.(event)

    resp =
      cond do
        # we have already processed the event
        # likely due to an unexpected restart, we want to skip over this event
        cursor >= event_cursor ->
          Logger.warn("Skipping over event #{event_cursor}. Already processed.")
          {:skip, EventOp.skip(event_cursor, event, Timespan.stop(timespan))}

        # if there was an error at a previous moment, we don't want to call handle_event
        # ever again for this partition
        status == :error ->
          {:skip, EventOp.skip(event_cursor, event, Timespan.stop(timespan))}

        status == :ok ->
          try do
            ops = handle_event.(event)
            {:ok, EventOp.new(event_cursor, event, ops, Timespan.stop(timespan))}
          rescue
            error ->
              {:error, EventOp.error(event_cursor, event, error, Timespan.stop(timespan))}
          end
      end

    case resp do
      {status, event_op} when status in [:ok, :skip] ->
        do_reduce(rest, MultiOp.add(multi, event_op), {handle_event, get_cursor}, on_error)

      {:error, event_op} ->
        do_reduce(
          rest,
          MultiOp.failed_on_event(multi, event_op),
          {handle_event, get_cursor},
          on_error
        )
    end
  end
end
