defmodule Derive.EventBatch do
  @type t :: %__MODULE__{
          events: [Derive.EventLog.event()],
          logger: Derive.Logger.t() | nil
        }
  defstruct [:events, :logger]
end
