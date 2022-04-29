defmodule Derive.State.EventOp do
  @moduledoc """
  An event and the operation that resulted from calling `handle_event` on the event.
  """

  @type t :: %__MODULE__{
          event: Derive.EventLog.event(),
          operation: Derive.Reducer.operation(),
          status: status(),
          error: any(),
          timing: timing() | nil
        }
  defstruct [:event, :operation, :status, :error, :timing]

  @type status() :: :ok | :error

  @type timing() :: {timestamp(), timestamp()} | {timestamp(), nil}

  @type timestamp :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}

  def new(event, ops, timing \\ nil) do
    %__MODULE__{
      status: :ok,
      event: event,
      operation: List.wrap(ops),
      timing: timing
    }
  end

  def error(event, error, timing \\ nil) do
    %__MODULE__{
      status: :error,
      event: event,
      error: error,
      timing: timing
    }
  end
end
