defmodule Derive.State.InMemory do
  use GenServer

  def start_link(opts) do
    {reducer_opts, genserver_opts} = Keyword.split(opts, [:reduce])
    GenServer.start_link(__MODULE__, reducer_opts, genserver_opts)
  end

  def get_state(pid), do: GenServer.call(pid, :get_state)

  def commit(pid, operations) do
    GenServer.call(pid, {:handle_operations, operations})
  end

  def init(opts) do
    reduce = Keyword.fetch!(opts, :reduce)
    {:ok, %{reduce: reduce, acc: %{}}}
  end

  def handle_call({:commmit, operations}, _from, %{reduce: reduce, acc: acc} = state) do
    new_acc = Enum.reduce(operations, acc, reduce)
    {:reply, :ok, %{state | acc: new_acc}}
  end

  def handle_call(:get_state, _from, %{acc: acc} = state) do
    {:reply, acc, state}
  end
end
