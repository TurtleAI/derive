defmodule Derive.Reducer do
  @type t :: module()

  @type partition() :: binary()

  @moduledoc """
  Defines how a given state is kept up to date based on an event source by a `Derive.Dispatcher`

  It happens as follows:
  - Events come from a source process defined by `&Derive.Reducer.source/0`
  - These are partitioned by `&Derive.Reducer.partition/1` for maximum concurrency
  - These events are processed by `&Derive.Reducer.handle_event/1`
    which produces 0+ operations that are meant to update some state
  - These operations are committed by &Derive.Reducer.commit_operations/1
  """

  @typedoc """
  A struct that represents a side-effect to be committed.

  `Derive.Reducer.commit_operations/1` will define how a batch of operations should be committed.
  """
  @type operation() :: any()

  @typedoc """
  The cursor pointing to a last event processed
  """
  @type version() :: String.t()

  @doc """
  The source where events from
  """
  @callback source() :: pid()

  @doc """
  Events within the same partition are processed in order.
  For example, returning event.user_id would guarantee that all events for a given user are processed in order.

  A partition is also used to maximize concurrency so events are processed as fast as possible.
  Events in different partitions can be processed simultaneously since they have no dependencies on one another.
  """
  @callback partition(Derive.EventLog.event()) :: partition() | nil

  @doc """
  For a given event, return a operation that should be run as a result.
  This is usually for keeping state up to date.

  How the operation is processed depends on the sink.
  """
  @callback handle_event(Derive.EventLog.event()) :: operation()

  @doc """
  Execute the operations that come from handle_event.
  These events will be processed in batches.
  """
  @callback commit_operations([operation()]) :: :ok

  @doc """
  Get the current overall version of the reducer
  """
  @callback get_version(partition()) :: version()

  @doc """
  Update the overall version of the dispatcher
  """
  @callback set_version(partition(), version()) :: :ok

  @doc """
  Reset the state so we can start processing from the first event
  This operation should reset the state in *all* partitions
  """
  @callback reset_state() :: :ok

  defmacro __using__(_options) do
    quote do
      @behaviour Derive.Reducer
    end
  end
end
