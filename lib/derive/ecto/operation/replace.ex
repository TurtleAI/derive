defmodule Derive.Ecto.Operation.Replace do
  @moduledoc """
  Replace an entire record in the database based on its primary key.

  If the record doesn't exist, this will be equivalent to an insert
  If the record already exists, this will replace all the fields in the struct
  """

  @type t :: %__MODULE__{
          record: Ecto.Schema.t()
        }

  defstruct [:record]
end

defimpl Derive.Ecto.DbOp, for: Derive.Ecto.Operation.Replace do
  def to_multi(
        %Derive.Ecto.Operation.Replace{record: %type{} = record},
        name
      ) do
    conflict_target = type.__schema__(:primary_key)
    fields = type.__schema__(:fields)

    on_conflict =
      cond do
        # if there are no additional fields, we don't need to do anything
        # {:replace_all_except, []}
        conflict_target == fields -> :nothing
        true -> {:replace_all_except, conflict_target}
      end

    Ecto.Multi.insert(Ecto.Multi.new(), name, record,
      returning: false,
      conflict_target: conflict_target,
      on_conflict: on_conflict
    )
  end
end
