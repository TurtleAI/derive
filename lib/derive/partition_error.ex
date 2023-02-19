defmodule Derive.PartitionError do
  @moduledoc """
  Represents the info about the state of a partition.
  """

  @type t :: %__MODULE__{
          type: type(),
          message: binary(),
          batch: [cursor()],
          cursor: cursor()
        }
  defstruct [:type, :message, :batch, :cursor]

  @typedoc """
  If the status of a partition is :ok, it is in an active state and can keep catching up
  If the status is :error, there was an error and no further processing is allowed

  `:handle_event` means there was an uncaught exception in the handle_event handler that
  is supposed to produce an event

  `:commit` means there is an error in committing one or more events/effects that was produced
  by `:handle_event`
  """
  @type type() :: :handle_event | :commit

  @typedoc """
  The cursor at which the error occurred.
  Usually points to the event which caused the error.
  """
  @type cursor() :: Derive.EventLog.cursor()
end
