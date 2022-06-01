defmodule Derive.EventBatch do
  @type t :: %__MODULE__{
          events: [Derive.EventLog.event()],
          logger: Derive.Logger.t() | nil,
          global_partition: Derive.Partition.t()
        }
  defstruct [:events, :logger, :global_partition]
end
