defmodule Derive.State.MultiOp do
  @moduledoc """
  `Derive.State.MultiOp` is a data structure for grouping multiple generic operations
  produced by combining the operations produced by `handle_event` over a list of events.

  Inspired by `Ecto.Multi` but generic to other types of operations.
  """

  alias Derive.{Partition, PartitionError}
  alias Derive.State.{MultiOp, EventOp}

  @type t :: %__MODULE__{
          partition: Partition.t(),
          initial_partition: Partition.t(),
          error: error() | nil,
          status: status(),
          operations: [EventOp.t()]
        }
  defstruct [:partition, :initial_partition, :error, status: :processing, operations: []]

  @typedoc """
  When processing events, there are 3 stages
  - processing: not all events have been processed
  - processed: the events have been processed (handle_event has been called)
  - committed: the operations from handle_event have been committed
  - error: the processing or committing has failed (an exception has been raised)
  """
  @type status :: :processing | :processed | :committed | :error

  @typedoc """
  An operation can fail when processing an event (calling handle_event)
  or during a commit.
  """
  @type error ::
          {:commit, commit_error()}
          | {:handle_event, EventOp.t()}

  @type commit_error() :: term()

  @doc """
  There are no operations, so committing this would be a no-op
  """
  def empty?(%MultiOp{operations: []}), do: true
  def empty?(%MultiOp{}), do: false

  @spec new(Partition.t()) :: MultiOp.t()
  def new(partition),
    do: %MultiOp{partition: partition, initial_partition: partition}

  @doc """
  Add an event operation that results from calling handle_event(event)
  """
  @spec add(MultiOp.t(), EventOp.t()) :: MultiOp.t()
  def add(
        %MultiOp{partition: partition, operations: operations} = multi,
        %EventOp{cursor: cursor} = op
      ) do
    new_partition = %{partition | cursor: max(cursor, partition.cursor)}
    new_operations = [op | operations]
    %{multi | partition: new_partition, operations: new_operations}
  end

  @doc """
  All of the events have been processed with handle_event, but they have
  not yet been committed
  """
  @spec processed(MultiOp.t()) :: MultiOp.t()
  def processed(%MultiOp{status: :processing} = multi),
    do: %{multi | status: :processed}

  @doc """
  The events have been processed and committed.
  There is nothing more eto be done.
  """
  @spec committed(MultiOp.t()) :: MultiOp.t()
  def committed(%MultiOp{status: :processed} = multi),
    do: %{multi | status: :committed}

  @doc """
  This operation failed on a particular handle_event(event)
  """
  @spec failed_on_event(MultiOp.t(), EventOp.t()) :: MultiOp.t()
  def failed_on_event(
        %MultiOp{partition: partition} = multi,
        %EventOp{cursor: cursor, error: error} = op
      ) do
    partition_error = %PartitionError{
      type: :handle_event,
      cursor: partition.cursor,
      message: inspect(error)
    }

    new_partition = %Partition{
      partition
      | status: :error,
        cursor: max(cursor, partition.cursor),
        error: partition_error
    }

    %{
      multi
      | status: :error,
        partition: new_partition,
        error: {:handle_event, op}
    }
  end

  @doc """
  This operation failed during the commit phase
  """
  @spec commit_failed(MultiOp.t(), commit_error()) :: MultiOp.t()
  def commit_failed(%MultiOp{partition: partition} = multi, error) do
    %{
      multi
      | status: :error,
        partition: %{partition | status: :error},
        error: {:commit, error}
    }
  end

  @doc """
  A flat list of the operations that can be committed
  In the order that they were added
  """
  @spec operations(MultiOp.t()) :: [EventOp.t()]
  def operations(%MultiOp{operations: operations}) do
    operations
    |> Enum.reverse()
    |> Enum.flat_map(fn %EventOp{operations: ops} -> ops end)
  end
end
