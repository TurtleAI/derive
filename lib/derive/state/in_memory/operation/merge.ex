defmodule Derive.State.InMemory.Operation.Merge do
  defstruct [:selector, :attrs]
end

defimpl Derive.State.InMemory.Reduce, for: Derive.State.InMemory.Operation.Merge do
  def reduce(%{selector: selector, attrs: attrs}, acc) do
    update_at(acc, selector, fn
      nil -> attrs
      record -> Map.merge(record, attrs)
    end)
  end

  defp update_at(map, {type, id}, update) when is_map(map) and is_function(update, 1) do
    case map do
      %{^type => %{^id => record} = record_map} ->
        Map.put(map, type, Map.put(record_map, id, update.(record)))

      %{^type => record_map} ->
        Map.put(map, type, Map.put(record_map, id, update.(nil)))

      map ->
        Map.put(map, type, %{id => update.(nil)})
    end
  end
end
