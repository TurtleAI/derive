defmodule Derive.State.EventOp do
  @type t :: %__MODULE__{
          event: Derive.EventLog.event(),
          operation: Derive.Reducer.operation(),
          status: status(),
          error: any()
        }
  defstruct [:event, :operation, :status, :error]

  @type status() :: :ok | :error
end
