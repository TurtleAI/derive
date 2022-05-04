defmodule Derive.Reducer do
  @moduledoc """
  Defines how a given state is kept up to date based on an event source by a `Derive.Dispatcher`

  It happens as follows:
  - Events come from a source process as configured in `Derive.Dispatcher`
  - These are partitioned by `&Derive.Reducer.partition/1` for maximum concurrency
  - These events are processed by `&Derive.Reducer.handle_event/1`
    which produces 0+ operations that are meant to update some state
  - These operations are committed by &Derive.Reducer.commit/1
  """

  alias Derive.{EventLog, Partition}
  alias Derive.State.MultiOp

  @type t :: module()

  @typedoc """
  A struct that represents a side-effect to be committed.

  `Derive.Reducer.commit/1` will define how a batch of operations should be committed.
  """
  @type operation() :: any()

  @typedoc """
  The cursor pointing to a last event processed
  """
  @type cursor() :: String.t()

  @type error_mode() :: :skip | :halt

  @type event_handler() :: (EventLog.event() -> operation() | nil)

  @doc """
  Events within the same partition are processed in order.
  For example, returning event.user_id would guarantee that all events for a given user are processed in order.

  A partition is also used to maximize concurrency so events are processed as fast as possible.
  Events in different partitions can be processed simultaneously since they have no dependencies on one another.
  """
  @callback partition(EventLog.event()) :: Partition.id() | nil

  @doc """
  For a given event, return a operation that should be run as a result.
  This is usually for keeping state up to date.

  How the operation is processed depends on the sink.
  """
  @callback handle_event(EventLog.event()) :: operation()

  @doc """
  Process all events. This typically means:
  - Call handle_event(event) on all events
  - Combine the operations
  - Commit those operations in a single step

  Returns a new MultiOp that reflects the operation that was committed.
  """
  @callback process_events([EventLog.event()], MultiOp.t()) :: MultiOp.t()

  @doc """
  Whether the event has already been processed.

  To handle unpredictable error scenarios, we need to make it possible for Derive
  to skip over already processed events.

  For example, imagine backend shuts down after a transaction is committed but before
  the `Derive.Dispatcher` updates its version to the latest one. Then the dispatcher
  will resend some events that were already processed by the partitions and the
  reducer will need to skip over them.
  """
  @callback processed_event?(Partition.t(), EventLog.event()) :: boolean()

  @doc """
  Reset the state so we can start processing from the first event
  This operation should reset the state in *all* partitions
  """
  @callback reset_state() :: :ok

  @doc """
  Get the current overall partition record
  """
  @callback get_partition(Derive.Partition.id()) :: Derive.Partition.t()

  @doc """
  Persist the partition record
  """
  @callback set_partition(Derive.Partition.t()) :: :ok

  defmacro __using__(_options) do
    quote do
      @behaviour Derive.Reducer
    end
  end
end
