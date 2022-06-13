defmodule Derive.Partition do
  @moduledoc """
  Represents the info about the state of a partition.
  """

  @type t :: %__MODULE__{
          id: id(),
          cursor: Derive.Reducer.cursor(),
          status: status(),
          error: error() | nil
        }
  defstruct [:id, :cursor, :status, :error]

  alias Derive.PartitionError

  @typedoc """
  If the status of a partition is :ok, it is in an active state and can keep catching up
  If the status is :error, there was an error and no further processing is allowed
  """
  @type status() :: :ok | :error

  @type error() :: PartitionError.t()

  @typedoc """
  The id of the partition. Can be any string.
  Originates from `Derive.Reducer.partition(...)`
  """
  @type id() :: binary()

  def to_string(%__MODULE__{id: id, cursor: cursor, status: status}) do
    "#{id}: #{cursor} [#{status}]"
  end

  @doc """
  The reserved id for a partition record that keeps track of overall
  state for all the partitions within a reducer
  """
  def global_id, do: "$"

  @doc """
  The reserved id for a partition record that keeps track of a version
  of the reducer/state combination.

  If this partition doesn't match what is configured in the reducer,
  a rebuild is needed.
  """
  def version_id, do: "$version"
end
