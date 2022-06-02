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
  @type operation() :: term()

  @type event() :: EventLog.event()

  @typedoc """
  The cursor pointing to a last event processed
  """
  @type cursor() :: EventLog.cursor()

  @doc """
  Events within the same partition are processed in order.
  For example, returning event.user_id would guarantee that all events for a given user are processed in order.

  A partition is also used to maximize concurrency so events are processed as fast as possible.
  Events in different partitions can be processed simultaneously since they have no dependencies on one another.
  """
  @callback partition(event()) :: Partition.id() | nil

  @doc """
  For a given event, return a operation that should be run as a result.
  This is usually for keeping state up to date.

  How the operation is processed depends on the sink.
  """
  @callback handle_event(event()) :: operation()

  @doc """
  Process all events. This typically means:
  - Call handle_event(event) on all events
  - Combine the operations
  - Commit those operations in a single step

  Returns a new MultiOp that reflects the operation that was committed.
  """
  @callback process_events([event()], MultiOp.t()) :: MultiOp.t()

  @doc """
  For a given event, return the cursor for the event.
  """
  @callback get_cursor(event()) :: cursor()

  @doc """
  Load the partition from where it is persisted.
  For example can be in Postgres, in memory, or elsewhere.
  This will be called at the start of a process booting.
  """
  @callback load_partition(Partition.id()) :: Partition.t()

  @doc """
  Persist a partition.
  This may be called liberally such as every time it is updated in memory.
  """
  @callback save_partition(Derive.Partition.t()) :: :ok

  @doc """
  Optional children to include as part of the Derive supervision tree.
  Only used for advanced cases and specialized implementations of `Derive.Reducer`
  """
  @callback child_specs(atom()) :: [Supervisor.child_spec()]

  defmacro __using__(_options) do
    quote do
      @behaviour Derive.Reducer

      def child_specs(_), do: []
      defoverridable child_specs: 1
    end
  end

  @doc """
  Whether a module implements the `Derive.Reducer` behavior
  """
  @spec implemented?(atom()) :: boolean()
  def implemented?(module) do
    behaviours = Keyword.get(module.__info__(:attributes), :behaviour, [])
    Enum.member?(behaviours, __MODULE__)
  end
end
