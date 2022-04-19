defmodule Derive.State.Ecto.Operation.SetPartition do
  @moduledoc """
  In state backed by Ecto, we want to update the latest version of a partition
  within a transaction to guarantee consistency.

  This way, the state is update and its partition version is updated within the same transaction.
  """

  defstruct [:table, :partition]
end

defimpl Derive.State.Ecto.DbOp, for: Derive.State.Ecto.Operation.SetPartition do
  def to_multi(%Derive.State.Ecto.Operation.Insert{record: record, on_conflict: :raise}, index) do
    Ecto.Multi.insert(Ecto.Multi.new(), index, record, returning: false)
  end

  def to_multi(
        %Derive.State.Ecto.Operation.SetPartition{
          table: table,
          partition: %{id: id, version: version, status: status}
        },
        index
      ) do
    status = status_to_int(status)
    table_name = table.__schema__(:source)

    version_record = [id: id, version: version, status: status]

    on_conflict = [set: [version: version, status: status]]

    Ecto.Multi.insert_all(Ecto.Multi.new(), index, table_name, [version_record],
      on_conflict: on_conflict,
      conflict_target: [:id]
    )
  end

  defp status_to_int(:ok), do: 1
  defp status_to_int(:error), do: 2
end
