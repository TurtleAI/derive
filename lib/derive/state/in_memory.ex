defmodule Derive.State.InMemory do
  use GenServer

  @moduledoc """
  An in-memory implementation of state
  """

  def start_link(opts) do
    {reducer_opts, genserver_opts} = Keyword.split(opts, [:reduce])
    GenServer.start_link(__MODULE__, reducer_opts, genserver_opts)
  end

  def get_state(server),
    do: GenServer.call(server, :get_state)

  def reset_state(server),
    do: GenServer.call(server, :reset_state)

  def commit(server, operations),
    do: GenServer.call(server, {:commit, operations})

  def init(opts) do
    reduce = Keyword.fetch!(opts, :reduce)
    {:ok, %{reduce: reduce, acc: %{}}}
  end

  def handle_call({:commit, operations}, _from, %{reduce: reduce, acc: acc} = state) do
    new_acc = Enum.reduce(operations, acc, reduce)
    {:reply, :ok, %{state | acc: new_acc}}
  end

  def handle_call(:reset_state, _from, state) do
    {:reply, :ok, %{state | acc: %{}}}
  end

  def handle_call(:get_state, _from, %{acc: acc} = state) do
    {:reply, acc, state}
  end
end
