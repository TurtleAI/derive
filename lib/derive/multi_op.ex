defmodule Derive.MultiOp do
  @moduledoc """
  `Derive.MultiOp` that maintains the current state of processing a batch of events
  within a given partition.

  - A batch of events is processed with handle_event(event) and produces operations
  - Those operations are committed in a single shot
  """

  alias Derive.{Partition, MultiOp, EventOp}
  alias Derive.Error.{HandleEventError, CommitError}

  @type t :: %__MODULE__{
          partition: Partition.t(),
          initial_partition: Partition.t(),
          error: error() | nil,
          save_partition: Derive.Reducer.operation() | nil,
          status: status(),
          operations: [EventOp.t()]
        }
  defstruct [
    :partition,
    :initial_partition,
    :error,
    :save_partition,
    status: :processing,
    operations: []
  ]

  @typedoc """
  When processing events, there are 3 stages
  - processing: not all events have been processed
  - processed: the events have been processed (handle_event has been called)
  - committed: the operations from handle_event have been committed
  - error: the processing or committing has failed (an exception has been raised)
  - skipped: the events have been skipped due to an error
  """
  @type status :: :processing | :processed | :committed | :error | :skipped

  @typedoc """
  An operation can fail when processing an event (calling handle_event)
  or during a commit.
  """
  @type error :: HandleEventError.t() | CommitError.t()

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
        %EventOp{cursor: cursor, status: :ok} = op
      ) do
    new_partition = %{partition | cursor: max(cursor, partition.cursor)}
    new_operations = [op | operations]
    %{multi | partition: new_partition, operations: new_operations}
  end

  def add(
        %MultiOp{operations: operations} = multi,
        %EventOp{status: :ignore} = op
      ) do
    # we don't update the cursor if we're ignoring the event
    %{multi | operations: [op | operations]}
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
  Due to an error (partition was halted) the events have been skipped
  """
  @spec skipped(MultiOp.t()) :: MultiOp.t()
  def skipped(%MultiOp{status: :skipped} = multi),
    do: %{multi | status: :skipped}

  @spec failed(MultiOp.t(), any()) :: MultiOp.t()
  def failed(multi, error) do
    %MultiOp{multi | status: :error, error: error}
    |> fail_partition()
  end

  defp fail_partition(%MultiOp{partition: partition, error: %HandleEventError{} = e} = multi) do
    partition_error = HandleEventError.to_partition_error(e, multi)
    %MultiOp{multi | partition: %Partition{partition | status: :error, error: partition_error}}
  end

  defp fail_partition(
         %MultiOp{
           initial_partition: %Partition{cursor: cursor_before_commit},
           partition: partition,
           error: %CommitError{} = e
         } = multi
       ) do
    partition_error = CommitError.to_partition_error(e, multi)

    %MultiOp{
      multi
      | partition: %Partition{
          partition
          | status: :error,
            error: partition_error,
            # if we ever resume operations, we want the cursor to be reverted to the previous commit
            cursor: cursor_before_commit
        }
    }
  end

  @doc """
  Add a special operation that's meant to update the partition along with all of the
  other operations.

  This method is preferred because you can ensure this all happens in a single transaction
  to prevent edge cases for the cursor getting out of sync.
  """
  @spec save_partition(MultiOp.t(), Derive.Reducer.operation()) :: MultiOp.t()
  def save_partition(
        %MultiOp{
          save_partition: nil
        } = multi,
        operation
      ) do
    %MultiOp{multi | save_partition: operation}
  end

  @doc """
  A list of `EventOp` operations, each of which corresponds to a
  single `handle_event` call.
  Mainly, each `EventOp` contains the event that was passed into `handle_event`
  and the operations that were emitted from it.
  """
  @spec event_operations(MultiOp.t()) :: [EventOp.t()]
  def event_operations(%MultiOp{operations: operations}),
    do: Enum.reverse(operations)

  @doc """
  A flat list of the operations that can be committed
  Same order as the order that they were added
  """
  @spec operations(MultiOp.t()) :: [Derive.Reducer.operation()]
  def operations(%MultiOp{} = multi) do
    for %EventOp{operations: operations} <- event_operations(multi), op <- operations, do: op
  end

  @doc """
  Get an event operation based on the index of the operation
  """
  @spec find_event_op_by_index(MultiOp.t(), integer()) :: EventOp.t()
  def find_event_op_by_index(%MultiOp{operations: operations}, index) do
    operations
    |> Enum.reverse()
    |> do_find_event_op_by_index(index)
  end

  defp do_find_event_op_by_index([], _index),
    do: nil

  defp do_find_event_op_by_index([%EventOp{operations: operations} = event_op | rest], index) do
    op_count = Enum.count(operations)

    if index < op_count do
      event_op
    else
      do_find_event_op_by_index(rest, index - op_count)
    end
  end
end
