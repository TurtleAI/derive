defmodule Derive.State.InMemory do
  use GenServer

  @moduledoc """
  An in-memory implementation of state
  """

  defstruct [:reduce, :acc]

  alias Derive.State.InMemory, as: S
  alias Derive.MultiOp

  def start_link(opts) do
    {reducer_opts, genserver_opts} = Keyword.split(opts, [:reduce])
    GenServer.start_link(__MODULE__, reducer_opts, genserver_opts)
  end

  ### Client

  def get_state(server),
    do: GenServer.call(server, :get_state)

  def reset_state(server),
    do: GenServer.call(server, :reset_state)

  def commit(server, %MultiOp{} = op) do
    GenServer.call(server, {:commit, op})
  end

  ### Server

  def init(opts) do
    reduce = Keyword.fetch!(opts, :reduce)
    {:ok, %S{reduce: reduce, acc: %{}}}
  end

  def handle_call({:commit, op}, _from, %S{reduce: reduce, acc: acc} = state) do
    operations = MultiOp.operations(op)
    new_acc = Enum.reduce(operations, acc, reduce)
    {:reply, MultiOp.committed(op), %{state | acc: new_acc}}
  end

  def handle_call(:reset_state, _from, state) do
    {:reply, :ok, %{state | acc: %{}}}
  end

  def handle_call(:get_state, _from, %S{acc: acc} = state) do
    {:reply, acc, state}
  end
end
