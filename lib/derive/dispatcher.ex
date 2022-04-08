defmodule Derive.Dispatcher do
  use GenServer

  alias Derive.PartitionDispatcher

  @moduledoc """
  Responsible for keeping derived state up to date based on implementation `Derive.Reducer`

  Events are processed async, meaning whatever state they update is eventually consistent.
  Events are processed concurrently, but in order based on Derive.Reducer.partition/1
  """

  def start_link(reducer, opts \\ []) do
    {_dispatcher_opts, genserver_opts} =
      opts
      |> Keyword.split([])

    GenServer.start_link(__MODULE__, %{reducer: reducer}, genserver_opts)
  end

  ### Client

  @doc """
  Wait for all of the events to be processed by all of the matching partitions as defined by
  `Derive.Reducer.partition/1`

  If the event has already been processed, this will complete immediately
  If the event has not yet been processed, this will block until it completes processing

  Events are not considered processed until *all* operations produced by `Derive.Reducer.handle_event/1`
  have been committed by `Derive.Reducer.commit_operations/1`
  """
  def await(dispatcher, events),
    do: GenServer.call(dispatcher, {:await, events})

  def init(%{reducer: reducer}) do
    Process.flag(:trap_exit, true)

    Derive.EventLog.subscribe(reducer.source(), self())

    {:ok, %{reducer: reducer}}
  end

  ### Server

  def handle_call({:await, events}, _from, %{reducer: reducer} = state) do
    List.wrap(events)
    |> events_by_partition_dispatcher(reducer)
    |> Enum.each(fn {partition_dispatcher, events} ->
      PartitionDispatcher.await(partition_dispatcher, events)
    end)

    {:reply, :ok, state}
  end

  def handle_cast({:new_events, events}, %{reducer: reducer} = state) do
    events
    |> events_by_partition_dispatcher(reducer)
    |> Enum.each(fn {partition_dispatcher, events} ->
      PartitionDispatcher.dispatch_events(partition_dispatcher, events)
    end)

    {:noreply, state}
  end

  def handle_info({:EXIT, _, :normal}, state) do
    {:stop, :shutdown, state}
  end

  defp events_by_partition_dispatcher(events, reducer) do
    events_by_partition = Enum.group_by(events, &reducer.partition/1)

    for {partition, events} <- events_by_partition, into: %{} do
      partition_dispatcher = Derive.PartitionSupervisor.lookup_or_start({reducer, partition})
      {partition_dispatcher, events}
    end
  end
end
