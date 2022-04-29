defmodule Derive.State.MultiOp do
  @moduledoc """
  `Derive.State.MultiOp` is a data structure for grouping multiple generic operations
  produced by combining the operations produced by `handle_event` over a list of events.

  Inspired by `Ecto.Multi` but generic to other types of operations.
  """

  alias Derive.State.{MultiOp, EventOp}

  @type t :: %__MODULE__{
          partition: Derive.Partition.t(),
          error: error() | nil,
          status: status(),
          operations: [Derive.State.EventOp.t()]
        }
  defstruct [:partition, :error, status: :processing, operations: []]

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
          {:commit, inner_error()}
          | {:handle_event, EventOp.t()}

  @type inner_error :: any()

  def empty?(%MultiOp{operations: []}), do: true
  def empty?(_), do: false

  def new(partition),
    do: %MultiOp{partition: partition}

  def add(%MultiOp{} = multi, _event, []),
    do: multi

  def add(%MultiOp{} = multi, _event, nil),
    do: multi

  def add(
        %MultiOp{partition: partition, operations: operations} = multi,
        %EventOp{event: event} = op
      ) do
    new_partition = %{partition | version: max(event.id, partition.version)}
    new_operations = [op | operations]
    %{multi | partition: new_partition, operations: new_operations}
  end

  def processed(%MultiOp{status: :processing} = multi),
    do: %{multi | status: :processed}

  def committed(%MultiOp{status: :processed} = multi),
    do: %{multi | status: :committed}

  @doc """
  This operation failed during the commit phase
  """
  def commit_failed(%MultiOp{partition: partition} = multi, error) do
    %{
      multi
      | status: :error,
        partition: %{partition | status: :error},
        error: {:commit, error}
    }
  end

  @doc """
  This operation failed on a particular handle_event
  """
  def failed_on_event(%MultiOp{partition: partition} = multi, event_op) do
    %{
      multi
      | status: :error,
        partition: %{partition | status: :error},
        error: {:handle_event, event_op}
    }
  end

  @doc """
  A flat list of the operations that can be committed
  """
  def operations(%MultiOp{operations: operations}) do
    Enum.flat_map(Enum.reverse(operations), fn
      %EventOp{status: :ok, operation: ops} -> ops
      %EventOp{status: :error} -> []
    end)
  end
end
