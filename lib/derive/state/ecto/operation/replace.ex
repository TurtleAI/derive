defmodule Derive.State.Ecto.Operation.Replace do
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

defimpl Derive.State.Ecto.DbOp, for: Derive.State.Ecto.Operation.Replace do
  def to_multi(
        %Derive.State.Ecto.Operation.Replace{record: %type{} = record},
        name
      ) do
    conflict_target = type.__schema__(:primary_key)

    Ecto.Multi.insert(Ecto.Multi.new(), name, record,
      returning: false,
      on_conflict: {:replace_all_except, conflict_target},
      conflict_target: conflict_target
    )
  end
end
