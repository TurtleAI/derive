defmodule Derive.Partition do
  defstruct [:id, :version, :status]

  @moduledoc """
  Represents the info about the state of a partition.
  """

  @typedoc """
  If the status of a partition is :ok, it is in an active state and can keep catching up
  If the status is :error, there was an error and no further processing is allowed
  """
  @type status() :: :ok | :error

  @type id() :: binary()

  @type t :: %__MODULE__{
          id: id(),
          version: Derive.Reducer.version(),
          status: status()
        }
end
