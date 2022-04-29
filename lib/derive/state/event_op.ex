defmodule Derive.State.EventOp do
  @moduledoc """
  An event and the operation that resulted from calling `handle_event` on the event.
  """

  @type t :: %__MODULE__{
          event: Derive.EventLog.event(),
          operations: Derive.Reducer.operation(),
          status: status(),
          error: any(),
          timing: Derive.Timing.t() | nil
        }
  defstruct [:event, :operations, :status, :error, :timing]

  @type status() :: :ok | :error

  def new(event, ops, timing \\ nil) do
    %__MODULE__{
      status: :ok,
      event: event,
      operations: List.wrap(ops),
      timing: timing
    }
  end

  def error(event, error, timing \\ nil) do
    %__MODULE__{
      status: :error,
      event: event,
      operations: [],
      error: error,
      timing: timing
    }
  end

  def empty?(%__MODULE__{operations: []}), do: true
  def empty?(%__MODULE__{}), do: false
end
