defmodule Derive.Ecto.Operation.Insert do
  @moduledoc """
  Insert a new record into the database

  You can think of it like:
  table.add(record)
  """

  defstruct [:record, on_conflict: :raise]
end

defimpl Derive.Ecto.DbOp, for: Derive.Ecto.Operation.Insert do
  def to_multi(
        %Derive.Ecto.Operation.Insert{record: %type{} = record, on_conflict: on_conflict},
        name
      ) do
    conflict_target = type.__schema__(:primary_key)
    unique_constraint_name = type.__schema__(:source) <> "_pkey"

    changeset =
      record
      |> Ecto.Changeset.change()
      |> Ecto.Changeset.unique_constraint(conflict_target, name: unique_constraint_name)

    opts = [returning: false]

    opts =
      case on_conflict do
        nil -> opts
        :raise -> Keyword.merge(opts, on_conflict: :raise)
        value -> Keyword.merge(opts, on_conflict: value, conflict_target: conflict_target)
      end

    Ecto.Multi.insert(Ecto.Multi.new(), name, changeset, opts)
  end
end
