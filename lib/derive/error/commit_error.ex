defmodule Derive.Error.CommitError do
  @moduledoc """
  Error when trying to commit some operations
  """

  defexception [:error, :stacktrace, :operation]

  @type t :: %__MODULE__{
          stacktrace: System.stacktrace() | nil,
          operation: Derive.State.EventOp.t() | nil
        }

  alias Derive.State.EventOp

  def message(%__MODULE__{operation: operation, error: error}) do
    "commit failed [#{operation.cursor}] #{inspect(error)}"
  end

  def to_partition_error(
        %__MODULE__{operation: operation, error: error, stacktrace: stacktrace},
        %Derive.State.MultiOp{
          operations: operations
        }
      ) do
    batch = for %EventOp{cursor: cursor} <- Enum.reverse(operations), do: cursor

    cursor =
      case operation do
        %EventOp{cursor: cursor} -> cursor
        nil -> nil
      end

    %Derive.PartitionError{
      type: :commit,
      batch: batch,
      cursor: cursor,
      message: Exception.format(:error, error, stacktrace || [])
    }
  end
end
