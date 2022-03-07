defmodule Derive.State.Ecto.Operation.Merge do
  defstruct [:selector, :attrs]
end

defimpl Derive.State.Ecto.DbOp, for: Derive.State.Ecto.Operation.Merge do
  import Derive.State.Ecto.Util

  def to_multi(%Derive.State.Ecto.Operation.Merge{selector: selector, attrs: fun} = merge, index)
      when is_function(fun) do
    Ecto.Multi.run(Ecto.Multi.new(), index, fn repo, _ ->
      record = repo.one(selector_query(selector))
      multi = to_multi(%{merge | attrs: fun.(record)}, index)
      repo.transaction(multi)
      {:ok, nil}
    end)
  end

  def to_multi(%Derive.State.Ecto.Operation.Merge{selector: [type, _id], attrs: attrs}, index) do
    conflict_target = type.__schema__(:primary_key)
    fields = type.__schema__(:fields)
    attrs = Map.take(attrs, fields) |> Map.to_list()

    Ecto.Multi.insert_all(Ecto.Multi.new(), index, type, [attrs],
      returning: false,
      on_conflict: [set: attrs],
      conflict_target: conflict_target
    )
  end
end
