defmodule Derive.State.EventOp do
  @moduledoc """
  An event and the operation that resulted from calling `handle_event` on the event.
  """

  @type t :: %__MODULE__{
          event: Derive.EventLog.event(),
          operation: Derive.Reducer.operation(),
          status: status(),
          error: any()
        }
  defstruct [:event, :operation, :status, :error]

  @type status() :: :ok | :error
end
