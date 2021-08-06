defmodule Derive.Source.EventLog do
  use GenServer

  defstruct [events: [], subscribers: []]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def append(log, events) do
    GenServer.call(log, {:append, events})
  end

  @impl true
  def init(:ok) do
    {:ok, %Derive.Source.EventLog{}}
  end

  @impl true
  def handle_call({:append, new_events}, _from, %{events: events} = state) do
    Process.send_after(self(), {:new_events, new_events}, 0)
    {:reply, :ok, %{state | events: events ++ new_events}}
  end

  def handle_call({:subscribe, new_subscriber}, _from, %{subscribers: subscribers} = state) do
    {:reply, :ok, %{state | subscribers: subscribers ++ [new_subscriber]}}
  end

  @impl true
  def handle_info({:new_events, new_events}, %{subscribers: subscribers}=state) do
    for sub <- subscribers do
      GenServer.cast(sub, {:new_events, new_events})
    end

    {:noreply, state}
  end

  def handle_info(:timeout, state) do
    {:stop, :normal, state}
  end

  def handle_info({:EXIT, _, :normal}, state) do
    {:stop, :shutdown, state}
  end

end
