defmodule Derive.Util do
  def update_at(map, [type, id], update) when is_map(map) and is_function(update, 1) do
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
