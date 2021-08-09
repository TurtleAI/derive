defmodule Derive.Sink.InMemory do
  use GenServer

  def start_link(opts) do
    {reducer_opts, genserver_opts} = Keyword.split(opts, [:reduce])
    GenServer.start_link(__MODULE__, reducer_opts, genserver_opts)
  end

  def fetch(sink), do: GenServer.call(sink, :fetch)

  def init(opts) do
    reduce = Keyword.fetch!(opts, :reduce)
    {:ok, %{reduce: reduce, acc: %{}}}
  end

  def handle_call({:handle_changes, changes}, _from, %{reduce: reduce, acc: acc} = state) do
    new_acc = Enum.reduce(changes, acc, reduce)
    {:reply, :ok, %{state | acc: new_acc}}
  end

  def handle_call(:fetch, _from, %{acc: acc} = state) do
    {:reply, acc, state}
  end
end

defprotocol Derive.Sink.InMemory.Reduce do
  @spec reduce(t, any()) :: any()
  def reduce(t, acc)
end

defimpl Derive.Sink.InMemory.Reduce, for: Derive.Reducer.Change.Merge do
  def reduce(%{selector: selector, attrs: attrs}, acc) do
    Derive.Util.update_at(acc, selector, fn
      nil -> attrs
      record -> Map.merge(record, attrs)
    end)
  end
end

defimpl Derive.Sink.InMemory.Reduce, for: Derive.Reducer.Change.Reset do
  def reduce(_, _acc) do
    %{}
  end
end
