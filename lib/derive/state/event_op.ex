defmodule Derive.State.EventOp do
  @moduledoc """
  An event and the operation that resulted from calling `handle_event` on the event.
  """

  @type t :: %__MODULE__{
          event: Derive.EventLog.event(),
          operations: Derive.Reducer.operation(),
          status: status(),
          error: any(),
          timespan: Derive.Timespan.t() | nil
        }
  defstruct [:event, :operations, :status, :error, :timespan]

  @type status() :: :ok | :error

  def new(event, ops, timespan \\ nil) do
    %__MODULE__{
      status: :ok,
      event: event,
      operations: List.wrap(ops),
      timespan: timespan
    }
  end

  def error(event, error, timespan \\ nil) do
    %__MODULE__{
      status: :error,
      event: event,
      operations: [],
      error: error,
      timespan: timespan
    }
  end

  def empty?(%__MODULE__{operations: []}), do: true
  def empty?(%__MODULE__{}), do: false
end
