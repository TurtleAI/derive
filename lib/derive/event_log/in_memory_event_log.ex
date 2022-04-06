defmodule Derive.EventLog.InMemoryEventLog do
  use GenServer

  defstruct events: [], subscribers: []

  @moduledoc """
  An ephemeral in-memory event log used just for testing purposes.
  """

  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def append(log, events) do
    :ok = GenServer.call(log, {:append, events})
  end

  @impl true
  def init(:ok) do
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_call(
        {:append, new_events},
        _from,
        %{subscribers: subscribers, events: events} = state
      ) do
    notify_subscribers(subscribers, new_events)
    {:reply, :ok, %{state | events: events ++ new_events}}
  end

  def handle_call({:subscribe, new_subscriber}, _from, %{subscribers: subscribers} = state) do
    {:reply, :ok, %{state | subscribers: subscribers ++ [new_subscriber]}}
  end

  @impl true
  def handle_info(:timeout, state) do
    {:stop, :normal, state}
  end

  def handle_info({:EXIT, _, :normal}, state) do
    {:stop, :shutdown, state}
  end

  defp notify_subscribers([], _events), do: :ok

  defp notify_subscribers([subscriber | rest], events) do
    GenServer.cast(subscriber, {:new_events, events})
    notify_subscribers(rest, events)
  end
end
