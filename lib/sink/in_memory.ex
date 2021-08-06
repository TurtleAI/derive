defmodule Derive.Sink.InMemory do
  use GenServer

  alias Derive.Reducer.Change.{Insert, Delete, Update, Merge}

  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def init(:ok) do
    {:ok, %{}}
  end

  def fetch(sink), do: GenServer.call(sink, :fetch)

  def handle_call({:handle_changes, changes}, _from, state) do
    new_state = handle_changes(state, changes)
    {:reply, :ok, new_state}
  end

  def handle_call(:fetch, _from, state) do
    {:reply, state, state}
  end

  defp handle_changes(state, changes) do
    Enum.reduce(changes, state, fn change, acc ->
      reduce(acc, change)
    end)
  end

  defp reduce(map, %Merge{selector: [type, id], attrs: attrs}) do
    Map.put(map, type, %{id => attrs})
  end
end
