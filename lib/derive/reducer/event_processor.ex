defmodule Derive.Reducer.EventProcessor do
  @moduledoc """
  Provides the process-agnostic logic for processing an ordered list of events.
  """

  require Logger

  defmodule Options do
    @type t :: %__MODULE__{
            handle_event: event_handler(),
            get_cursor: cursor_handler(),
            commit: commit_handler(),
            logger: Derive.Logger.t(),
            on_error: on_error()
          }
    defstruct [:handle_event, :get_cursor, :commit, :logger, on_error: :halt]

    @typedoc """
    Given an event, produce an operation for that event
    """
    @type event_handler :: (Derive.EventLog.event() -> Derive.Reducer.operation())

    @typedoc """
    For an event, return the cursor for that event
    This function should be efficient and deterministic. Meaning, calling this
    multiple times on the same event should return the same value.
    """
    @type cursor_handler :: (Derive.EventLog.event() -> Derive.EventLog.cursor())

    @typedoc """
    Commit all of the events produced by handle_event
    """
    @type commit_handler :: (MultiOp.t() -> MultiOp.t())

    @typedoc """
    What to do in case of an error in processing.
    An error could happen during handle_event or commit.
    """
    @type on_error :: :halt
  end

  @type event :: Derive.EventLog.event()
  @type cursor :: Derive.EventLog.cursor()
  @type operation :: Derive.Reducer.operation()

  alias Derive.{Partition, Timespan, MultiOp, EventOp}
  alias Derive.Error.{HandleEventError, CommitError}

  @doc """
  Process events for a given list of events. This involves the following steps:
  - Call handle_event/1 for each event and collect the operations
  - Commit those operations

  In case of an error, processing may stop early
  """
  @spec process_events(
          [event()],
          MultiOp.t(),
          Options.t()
        ) ::
          MultiOp.t()
  def process_events(
        _,
        %MultiOp{partition: %Partition{status: :error}} = multi,
        %Options{on_error: :halt}
      ) do
    MultiOp.processed(multi)
  end

  def process_events(
        events,
        multi,
        %Options{commit: commit, logger: logger, handle_event: handle_event} = options
      ) do
    case reduce_events(events, multi, options) do
      %MultiOp{status: :processed} = multi ->
        # we only commit a multi if it has successfully been processed
        try do
          case commit.(multi) do
            %MultiOp{status: :error, error: %CommitError{} = error} = multi ->
              multi = %MultiOp{multi | error: %CommitError{error | handle_event: handle_event}}
              Derive.Logger.error(logger, multi)
              multi

            %MultiOp{} = multi ->
              multi
          end
        rescue
          # Due to a programmer error, the commit handler raised an exception
          # We don't want this to bring down the app and instead handle this explicitly
          error ->
            commit_error = %CommitError{
              commit: commit,
              handle_event: handle_event,
              operations: MultiOp.event_operations(multi),
              error: error,
              stacktrace: __STACKTRACE__
            }

            multi = MultiOp.failed(multi, commit_error)

            Derive.Logger.error(logger, multi)

            multi
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

  @spec reduce_events([event()], MultiOp.t(), Options.t()) :: Derive.MultiOp.t()
  def reduce_events([], %MultiOp{status: :processing} = multi, _),
    do: MultiOp.processed(multi)

  def reduce_events([], %MultiOp{} = multi, _),
    do: multi

  def reduce_events(
        [event | rest],
        %MultiOp{partition: %Partition{status: status, cursor: partition_cursor} = partition} =
          multi,
        %Options{
          handle_event: handle_event,
          get_cursor: get_cursor,
          logger: logger
        } = options
      ) do
    timespan = Timespan.start()
    event_cursor = get_cursor.(event)

    resp =
      cond do
        # we have already processed the event
        # likely due to an unexpected restart, we want to skip over this event
        partition_cursor >= event_cursor ->
          Derive.Logger.log(
            logger,
            {:warn,
             "Skipping over event #{event_cursor}. Already processed. - #{Partition.to_string(partition)}"}
          )

          {:ignore, EventOp.ignore(event_cursor, event, Timespan.stop(timespan))}

        # if there was an error for a previous event, we don't want to call handle_event
        # ever again for this partition
        status == :error ->
          {:ignore, EventOp.ignore(event_cursor, event, Timespan.stop(timespan))}

        status == :ok ->
          try do
            ops = handle_event.(event)
            {:ok, EventOp.new(event_cursor, event, ops, Timespan.stop(timespan))}
          rescue
            # Due to a programmer error, the handle_event raised an exception
            # We don't want this to bring down the app and instead handle this explicitly
            error ->
              event_op =
                EventOp.error(
                  event_cursor,
                  event,
                  {error, __STACKTRACE__},
                  Timespan.stop(timespan)
                )

              handle_event_error = %HandleEventError{
                operation: event_op,
                handle_event: handle_event,
                event: event,
                error: error,
                stacktrace: __STACKTRACE__
              }

              {:error, handle_event_error}
          end
      end

    case resp do
      {status, event_op} when status in [:ok, :ignore] ->
        reduce_events(rest, MultiOp.add(multi, event_op), options)

      {:error, handle_event_error} ->
        multi = MultiOp.failed(multi, handle_event_error)
        Derive.Logger.error(logger, multi)
        reduce_events(rest, multi, options)
    end
  end
end
