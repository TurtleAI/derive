defmodule Derive.State.EventOp do
  defstruct [:event, :operation, :status, :error]

  @type t :: %__MODULE__{
          event: Derive.EventLog.event(),
          operation: Derive.Reducer.operation(),
          status: status(),
          error: any()
        }

  @type status() :: :ok | :error
end
