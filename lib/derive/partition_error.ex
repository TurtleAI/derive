defmodule Derive.PartitionError do
  @moduledoc """
  Represents the info about the state of a partition.
  """

  @type t :: %__MODULE__{
          type: type(),
          message: binary(),
          cursor: cursor()
        }
  defstruct [:type, :message, :cursor]

  @typedoc """
  If the status of a partition is :ok, it is in an active state and can keep catching up
  If the status is :error, there was an error and no further processing is allowed
  """
  @type type() :: :handle_event

  @typedoc """
  The cursor at which the error occurred.
  Usually points to the event which caused the error.
  """
  @type cursor() :: Derive.EventLog.cursor()
end
