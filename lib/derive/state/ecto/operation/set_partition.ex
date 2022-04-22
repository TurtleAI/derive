defmodule Derive.State.Ecto.Operation.SetPartition do
  @moduledoc """
  In state backed by Ecto, we want to update the latest version of a partition
  within a transaction to guarantee consistency.
  This way, the state is update and its partition version is updated within the same transaction.
  """

  defstruct [:table, :partition]
end

defimpl Derive.State.Ecto.DbOp, for: Derive.State.Ecto.Operation.SetPartition do
  alias Derive.State.Ecto.PartitionRecord

  def to_multi(
        %Derive.State.Ecto.Operation.SetPartition{
          table: table,
          partition: %{id: id, version: version, status: status}
        },
        index
      ) do
    record =
      %PartitionRecord{id: id, version: version, status: status}
      |> Ecto.put_meta(source: table)

    Ecto.Multi.insert(Ecto.Multi.new(), index, record,
      returning: false,
      on_conflict: {:replace_all_except, [:id]},
      conflict_target: [:id]
    )
  end
end
