defmodule Derive.Ecto.Operation.Update do
  defstruct [:selector, :fields]
end

defimpl Derive.Ecto.DbOp, for: Derive.Ecto.Operation.Update do
  import Derive.Ecto.Selector

  def to_multi(
        %Derive.Ecto.Operation.Update{selector: selector, fields: fun} = update,
        index
      )
      when is_function(fun) do
    Ecto.Multi.run(Ecto.Multi.new(), index, fn repo, _ ->
      record = repo.one(selector_query(selector))
      multi = to_multi(%{update | fields: fun.(record)}, index)
      repo.transaction(multi)
      {:ok, nil}
    end)
  end

  def to_multi(%Derive.Ecto.Operation.Update{selector: selector, fields: fields}, index) do
    Ecto.Multi.update_all(Ecto.Multi.new(), index, selector_query(selector),
      set: to_keyword_list(fields)
    )
  end

  defp to_keyword_list(list) when is_list(list), do: list
  defp to_keyword_list(map) when is_map(map), do: Enum.to_list(map)
end
