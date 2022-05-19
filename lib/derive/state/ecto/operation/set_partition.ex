defmodule Derive.State.Ecto.Operation.SetPartition do
  @moduledoc """
  Insert or update a partition record that's stored in the given table.

  In state backed by Ecto, we want to update the latest cursor of a partition
  within a transaction to guarantee consistency.

  If the state and the partition version is updated within the same transaction,
  the two can never get out of sync.
  """

  defstruct [:table, :partition]

  @type t :: %__MODULE__{
    table: binary(),
    partition: Derive.Partition.t()
  }
end

defimpl Derive.State.Ecto.DbOp, for: Derive.State.Ecto.Operation.SetPartition do
  alias Ecto.Multi

  alias Derive.State.Ecto.PartitionRecord

  def to_multi(
        %Derive.State.Ecto.Operation.SetPartition{
          table: table,
          partition: partition
        },
        index
      ) do
    record = PartitionRecord.from_partition(partition)
      |> Ecto.put_meta(source: table)

    Multi.insert(Multi.new(), index, record,
      returning: false,
      on_conflict: {:replace_all_except, [:id]},
      conflict_target: [:id]
    )
  end
end
