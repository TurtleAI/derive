defmodule Derive.Error.CommitError do
  @moduledoc """
  Error when trying to commit some operations
  """

  defexception [:commit, :handle_event, :operations, :failed_operation, :error, :stacktrace]

  @type t :: %__MODULE__{
          stacktrace: System.stacktrace() | nil,
          operations: [Derive.State.EventOp.t()]
        }

  alias Derive.EventOp

  def message(%__MODULE__{
        commit: commit,
        handle_event: handle_event,
        operations: operations,
        failed_operation: failed_operation,
        error: error,
        stacktrace: stacktrace
      }) do
    [
      "CommitError :: " <> inspect(commit),
      "\n",
      Exception.format(:error, error),
      "\n\n",
      "handle_event :: " <> inspect(handle_event),
      "\n",
      Enum.map_join(operations, "\n\n", &operations_message(&1, failed_operation)),
      "\n\n",
      "Exception:",
      "\n",
      Exception.format(:error, error, stacktrace)
    ]
    |> IO.iodata_to_binary()
  end

  defp operations_message(%EventOp{event: event, operations: operations}, failed_operation) do
    [
      inspect(event),
      "\n  [\n",
      Enum.map_join(operations, "\n,", fn
        ^failed_operation ->
          indent(inspect(failed_operation) <> "<==", "    ")

        event_op ->
          indent(inspect(event_op), "    ")
      end),
      "\n  ]"
    ]
    |> IO.iodata_to_binary()
  end

  defp indent(text, prefix) do
    text
    |> String.split("\n")
    |> Enum.map(fn line -> prefix <> line end)
    |> Enum.join("\n")
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
