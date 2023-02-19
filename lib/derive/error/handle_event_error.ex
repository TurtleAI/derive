defmodule Derive.Error.HandleEventError do
  @moduledoc """
  When processing events, a failure happens when there's an exception for a specific
  handle_event(event)

  If this happens, the failure happens before we even get to a commit step.
  """

  defexception [:operation, :handle_event, :event, :error, :stacktrace]

  @type t :: %__MODULE__{
          operation: Derive.EventOp.t(),
          handle_event: Derive.Reducer.EventProcessor.event_handler()
        }

  alias Derive.MultiOp
  alias Derive.EventOp

  def message(%__MODULE__{
        handle_event: handle_event,
        event: event,
        error: error,
        stacktrace: stacktrace
      }) do
    "handle_event #{inspect(handle_event)} failed on:\n#{inspect(event)}\n" <>
      Exception.format(:error, error, stacktrace)
  end

  def to_partition_error(
        %__MODULE__{
          operation: %EventOp{cursor: cursor, error: {error, stacktrace}}
        },
        %Derive.MultiOp{} = multi
      ) do
    batch = for %EventOp{cursor: cursor} <- MultiOp.event_operations(multi), do: cursor

    %Derive.PartitionError{
      type: :handle_event,
      batch: batch,
      cursor: cursor,
      message: Exception.format(:error, error, stacktrace || [])
    }
  end
end
