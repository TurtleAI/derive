defmodule Derive.Error.CommitError do
  @moduledoc """
  Error when trying to commit some operations
  """

  defexception [:commit, :operations, :failed_operation, :error, :stacktrace]

  @type t :: %__MODULE__{
          stacktrace: System.stacktrace() | nil,
          operations: [Derive.State.EventOp.t()]
        }

  alias Derive.EventOp

  def message(%__MODULE__{
        commit: commit,
        operations: operations,
        error: error,
        stacktrace: stacktrace
      }) do
    [
      "commit failed:\n",
      inspect(commit),
      "\noperations:",
      for(o <- operations, do: inspect(o) <> "\n"),
      "exception:\n",
      [Exception.format(:error, error, stacktrace)]
    ]
    |> IO.iodata_to_binary()
  end

  def to_partition_error(
        %__MODULE__{
          operations: operations,
          failed_operation: failed_operation,
          error: error,
          stacktrace: stacktrace
        },
        _
      ) do
    batch = for %EventOp{cursor: cursor} <- operations, do: cursor

    cursor =
      case failed_operation do
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
