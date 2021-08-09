defmodule Derive.Util.MapOfSets do
  def put(map, key, value) do
    Map.update(map, key, MapSet.new([value]), fn set ->
      MapSet.put(set, value)
    end)
  end

  def delete(map, key, value) do
    case map do
      %{^key => set} -> Map.put(map, key, MapSet.delete(set, value))
      %{} -> map
    end
  end
end
