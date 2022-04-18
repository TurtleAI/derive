defmodule Derive.State.MultiOp do
  @moduledoc """
  Represents a collection of operations produced by events

  partition: the partition the operations must be run on
  """

  alias Derive.State.MultiOp, as: Op

  @type event_operation ::
          {Derive.EventLog.event(), {:ok, Derive.Reducer.operation()}}
          | {Derive.EventLog.event(), {:error, any()}}

  @type t :: %Derive.State.MultiOp{
          partition: Derive.Reducer.partition(),
          event_operations: event_operation
        }

  defstruct [:partition, event_operations: []]

  def empty?(%Op{event_operations: []}), do: true
  def empty?(_), do: false

  def new(partition \\ nil, event_operations \\ []) do
    %Op{partition: partition, event_operations: Enum.reverse(event_operations)}
  end

  def add(%Op{} = multi, _event, []), do: multi
  def add(%Op{} = multi, _event, nil), do: multi

  def add(%Op{} = multi, event, operation),
    do: add_operation(multi, event, {:ok, List.wrap(operation)})

  def error(%Op{} = multi, event, error),
    do: add_operation(multi, event, {:error, error})

  defp add_operation(%Op{event_operations: event_operations} = multi, event, op) do
    %{multi | event_operations: [{event, op} | event_operations]}
  end

  @doc """
  A flat list of the operations that can be committed
  """
  def operations(%Op{event_operations: event_operations}) do
    Enum.flat_map(Enum.reverse(event_operations), fn
      {_e, {:ok, ops}} -> ops
      {_e, {:error, _}} -> []
    end)
  end

  @doc """
  The new version of the state once a commit has succeeded
  """
  def partition_version(%Op{event_operations: event_operations}) do
    Enum.map(event_operations, fn
      {%{id: id}, _} -> id
    end)
    |> Enum.max()
  end
end
