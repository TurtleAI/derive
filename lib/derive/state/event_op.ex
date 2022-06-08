defmodule Derive.State.EventOp do
  @moduledoc """
  An event and the operation that resulted from calling `handle_event` on the event.
  """

  @type t :: %__MODULE__{
          cursor: cursor(),
          event: event(),
          operations: operations(),
          status: status(),
          error: term(),
          timespan: Derive.Timespan.t() | nil
        }
  defstruct [:cursor, :event, :operations, :status, :error, :timespan]

  @typedoc """
  The cursor a partition would point to if this event was processed
  """
  @type cursor() :: Derive.EventLog.cursor()

  @typedoc """
  The event that is being processed
  """
  @type event() :: Derive.EventLog.event()

  @type error() :: {any(), Exception.stacktrace() | nil}

  @typedoc """
  The operations that were produced from calling `handle_event`
  """
  @type operations() :: [Derive.Reducer.operation()]

  @type status() :: :ok | :error | :ignore

  def new(cursor, event, ops, timespan \\ nil) do
    %__MODULE__{
      status: :ok,
      cursor: cursor,
      event: event,
      operations: List.wrap(ops),
      timespan: timespan
    }
  end

  @spec error(error(), event(), error(), Derive.Timespan.t() | nil) :: t()
  def error(cursor, event, error, timespan \\ nil) do
    %__MODULE__{
      status: :error,
      cursor: cursor,
      event: event,
      operations: [],
      error: error,
      timespan: timespan
    }
  end

  def ignore(cursor, event, timespan \\ nil) do
    %__MODULE__{
      status: :ignore,
      cursor: cursor,
      event: event,
      operations: [],
      timespan: timespan
    }
  end

  def empty?(%__MODULE__{operations: []}), do: true
  def empty?(%__MODULE__{}), do: false
end
