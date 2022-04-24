defmodule Derive.Partition do
  @moduledoc """
  Represents the info about the state of a partition.
  """

  defstruct [:id, :version, :status]

  @type t :: %__MODULE__{
          id: id(),
          version: Derive.Reducer.version(),
          status: status()
        }

  @typedoc """
  If the status of a partition is :ok, it is in an active state and can keep catching up
  If the status is :error, there was an error and no further processing is allowed
  """
  @type status() :: :ok | :error

  @typedoc """
  The id of the partition. Can be any string.
  Originates from `Derive.Reducer.partition(...)`
  """
  @type id() :: binary()
end
