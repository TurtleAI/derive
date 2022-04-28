defmodule Derive.State.Ecto.Operation.Merge do
  @moduledoc """
  Insert or merge a record into the database.
  Equivalent to an SQL upsert statement.

  You can think of it like:
  selected_record = selected_record.merge(fields)
  """

  defstruct [:selector, :fields]
end

defimpl Derive.State.Ecto.DbOp, for: Derive.State.Ecto.Operation.Merge do
  def to_multi(
        %Derive.State.Ecto.Operation.Merge{selector: {type, selector_fields}, fields: fields},
        name
      ) do
    conflict_target = type.__schema__(:primary_key)

    primary_key_fields_in_selector =
      case conflict_target do
        [pk_field] -> %{pk_field => selector_fields}
        pk_fields -> Map.take(selector_fields, pk_fields)
      end

    fields_to_insert = Map.merge(primary_key_fields_in_selector, fields)
    fields_to_set = fields |> Map.take(type.__schema__(:fields)) |> Map.to_list()

    Ecto.Multi.insert_all(Ecto.Multi.new(), name, type, [fields_to_insert],
      returning: false,
      on_conflict: [set: fields_to_set],
      conflict_target: conflict_target
    )
  end
end
