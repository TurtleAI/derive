defmodule Derive.Migration do
  alias Derive.Partition

  require Logger

  @doc """
  Mark the entire state of a reducer for rebuild
  """
  @spec mark_for_rebuild(Derive.Reducer.t()) :: :ok
  def mark_for_rebuild(reducer) do
    if reducer.needs_rebuild? do
      # do nothing since a reducer is already needing a rebuild
    else
      # set the cursor (version) to 0 to force a version mismatch and a rebuild
      reducer.set_partition(%Partition{id: Partition.version_id(), cursor: "0", status: :ok})
    end
  end
end
