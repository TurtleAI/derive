defmodule Derive.State.Ecto.Operation.Insert do
  @moduledoc """
  Insert a new record into the database

  You can think of it like:
  table.add(record)
  """

  defstruct [:record, on_conflict: :raise]
end

defimpl Derive.State.Ecto.DbOp, for: Derive.State.Ecto.Operation.Insert do
  def to_multi(%Derive.State.Ecto.Operation.Insert{record: record, on_conflict: :raise}, name) do
    Ecto.Multi.insert(Ecto.Multi.new(), name, record, returning: false)
  end

  def to_multi(
        %Derive.State.Ecto.Operation.Insert{record: record, on_conflict: on_conflict},
        name
      ) do
    conflict_target = record.__struct__().__schema__(:primary_key)

    Ecto.Multi.insert(Ecto.Multi.new(), name, record,
      returning: false,
      on_conflict: on_conflict,
      conflict_target: conflict_target
    )
  end
end
