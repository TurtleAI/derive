defmodule Derive.Error.HandleEventError do
  @moduledoc """
  When processing events, a failure happens when there's an exception for a specific
  handle_event(event)

  If this happens, the failure happens before we even get to a commit step.
  """

  defexception [:operation]

  @type t :: %__MODULE__{
          operation: Derive.EventOp.t()
        }

  alias Derive.EventOp

  def message(%__MODULE__{operation: operation}) do
    "handle_event failed #{inspect(operation)}"
  end

  def to_partition_error(
        %__MODULE__{
          operation: %EventOp{cursor: cursor, error: {error, stacktrace}}
        },
        %Derive.MultiOp{operations: operations}
      ) do
    batch = for %EventOp{cursor: cursor} <- Enum.reverse(operations), do: cursor

    %Derive.PartitionError{
      type: :handle_event,
      batch: batch,
      cursor: cursor,
      message: Exception.format(:error, error, stacktrace || [])
    }
  end
end
