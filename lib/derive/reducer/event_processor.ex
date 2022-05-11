defmodule Derive.Reducer.EventProcessor do
  @moduledoc """
  Provides the process-agnostic logic for processing an ordered list of events.
  """

  @type t :: %__MODULE__{
          handle_event: event_handler(),
          get_cursor: cursor_handler(),
          commit: commit_handler(),
          on_error: on_error()
        }
  defstruct [:handle_event, :get_cursor, :commit, on_error: :halt]

  @type event :: Derive.EventLog.event()
  @type cursor :: Derive.EventLog.cursor()
  @type operation :: Derive.Reducer.operation()

  @typedoc """
  What to do in case of an error in processing.
  An error could happen during handle_event or commit.
  """
  @type on_error :: :halt

  @type option :: {:on_error, on_error()}

  @typedoc """
  Given an event, produce an operation for that event
  """
  @type event_handler :: (event() -> operation())

  @typedoc """
  For an event, return the cursor for that event
  This function should be efficient and deterministic. Meaning, calling this
  multiple times on the same event should return the same value.
  """
  @type cursor_handler :: (event() -> cursor())

  @typedoc """
  Commit all of the events produced by handle_event
  """
  @type commit_handler :: (MultiOp.t() -> MultiOp.t())

  alias Derive.State.{EventOp, MultiOp}
  alias Derive.{Partition, Timespan}

  require Logger

  @doc """
  Process events for a given list of events. This involves the following steps:
  - Call handle_event/1 for each event and collect the operations
  - Commit those operations

  In case of an error, processing may stop early
  """
  @spec process_events(
          [event()],
          MultiOp.t(),
          t()
        ) ::
          MultiOp.t()
  def process_events(
        events,
        multi,
        %__MODULE__{commit: commit} = processor
      ) do
    case reduce_events(events, multi, processor) do
      %MultiOp{status: :processed} = multi ->
        # we only commit a multi if it has successfully been processed
        try do
          commit.(multi)
        rescue
          # Due to a programmer error, commit raised an exception
          # We don't want this to bring down the app and instead handle this explicitly
          error ->
            MultiOp.commit_failed(multi, error)
        end

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
          t()
        ) ::
          Derive.State.MultiOp.t()
  def reduce_events(events, multi, %__MODULE__{
        handle_event: handle_event,
        get_cursor: get_cursor,
        on_error: on_error
      }) do
    do_reduce(events, multi, {handle_event, get_cursor}, on_error)
  end

  defp do_reduce([], %MultiOp{status: :processing} = multi, _handlers, _),
    do: MultiOp.processed(multi)

  defp do_reduce([], %MultiOp{} = multi, _handlers, _),
    do: multi

  defp do_reduce(
         [event | rest],
         %MultiOp{partition: %Partition{status: status, cursor: partition_cursor} = partition} =
           multi,
         {handle_event, get_cursor},
         on_error
       ) do
    timespan = Timespan.start()
    event_cursor = get_cursor.(event)

    resp =
      cond do
        # we have already processed the event
        # likely due to an unexpected restart, we want to skip over this event
        partition_cursor >= event_cursor ->
          Logger.warn(
            "Skipping over event #{event_cursor}. Already processed. - #{Partition.to_string(partition)}"
          )

          {:skip, EventOp.skip(event_cursor, event, Timespan.stop(timespan))}

        # if there was an error for a previous event, we don't want to call handle_event
        # ever again for this partition
        status == :error ->
          {:skip, EventOp.skip(event_cursor, event, Timespan.stop(timespan))}

        status == :ok ->
          try do
            ops = handle_event.(event)
            {:ok, EventOp.new(event_cursor, event, ops, Timespan.stop(timespan))}
          rescue
            # Due to a programmer error, the handle_event raised an exception
            # We don't want this to bring down the app and instead handle this explicitly
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
