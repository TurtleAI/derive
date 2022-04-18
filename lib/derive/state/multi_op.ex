defmodule Derive.State.MultiOp do
  @moduledoc """
  Represents a collection of operations produced by events

  partition: the partition the operations must be run on
  """

  @type event_operation ::
          {Derive.EventLog.event(), {:ok, Derive.Reducer.operation()}}
          | {Derive.EventLog.event(), {:error, any()}}

  @type t :: %Derive.State.MultiOp{
          partition: Derive.Reducer.partition(),
          event_operations: event_operation
        }

  defstruct [:partition, event_operations: []]

  def empty?(%__MODULE__{event_operations: []}), do: true
  def empty?(_), do: false

  def new(partition \\ nil, event_operations \\ []) do
    %__MODULE__{partition: partition, event_operations: event_operations}
  end

  @doc """
  A flat list of the operations that can be committed
  """
  def operations(%__MODULE__{event_operations: event_operations}) do
    Enum.flat_map(event_operations, fn
      {_e, {:ok, ops}} -> ops
      {_e, {:error, _}} -> []
    end)
  end

  @doc """
  The new version of the state once a commit has succeeded
  """
  def partition_version(%__MODULE__{event_operations: event_operations}) do
    Enum.map(event_operations, fn
      {%{id: id}, _} -> id
    end)
    |> Enum.max()
  end
end
