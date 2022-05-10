defmodule Derive.Migration do
  alias Derive.Partition

  require Logger

  @doc """
  Mark the entire state of a reducer for rebuild
  """
  @spec mark_for_rebuild(Derive.Reducer.t()) :: :ok
  def mark_for_rebuild(reducer) do
    reducer.set_partition(%Partition{id: "$status", cursor: "invalidated", status: :ok})
  end

  @doc """
  Whether a reducer needs to be rebuilt
  Currently defined defined by '&mark_for_rebuild/1`
  """
  @spec needs_rebuild?(Derive.Reducer.t()) :: boolean()
  def needs_rebuild?(reducer) do
    case reducer.get_partition("$status") do
      nil -> false
      %Partition{cursor: "invalidated"} -> true
    end
  end
end
