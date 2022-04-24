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
  @type version() :: String.t()

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
  Execute the `handle_event` for all events and return a combined operation
  that needs be committed for the state to update.
  """
  @callback reduce_events([EventLog.event()], Partition.t()) :: MultiOp.t()

  @doc """
  Execute the operations that come from handle_event.
  These events will be processed in batches.
  """
  @callback commit(MultiOp.t()) :: :ok

  @doc """
  Whether the event has already been processed or not.
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
