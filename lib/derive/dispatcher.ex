defmodule Derive.Dispatcher do
  use GenServer

  alias Derive.PartitionDispatcher
  alias __MODULE__, as: D

  defstruct [:reducer, :version]

  @moduledoc """
  Responsible for keeping derived state up to date based on implementation `Derive.Reducer`

  Events are processed concurrently, but in order is guaranteed based on the result of Derive.Reducer.partition/1
  State is eventually consistent
  You can call `&Derive.Dispatcher.await/2` lets you wait for events to be finished processing.
  """

  def start_link(reducer, opts \\ []) do
    {_dispatcher_opts, genserver_opts} =
      opts
      |> Keyword.split([])

    GenServer.start_link(__MODULE__, %D{reducer: reducer}, genserver_opts)
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

    {:ok, %D{reducer: reducer}, {:continue, :ok}}
  end

  ### Server

  def handle_continue(:ok, %D{reducer: reducer} = state) do
    version = reducer.get_version()
    {:noreply, %{state | version: version}}
  end

  def handle_call({:await, events}, _from, %D{reducer: reducer} = state) do
    List.wrap(events)
    |> events_by_partition_dispatcher(reducer)
    |> Enum.each(fn {partition_dispatcher, events} ->
      PartitionDispatcher.await(partition_dispatcher, events)
    end)

    {:reply, :ok, state}
  end

  def handle_cast({:new_events, events}, %D{reducer: reducer} = state) do
    events
    |> events_by_partition_dispatcher(reducer)
    |> Enum.map(fn {partition_dispatcher, events} ->
      PartitionDispatcher.dispatch_events(partition_dispatcher, events)
      {partition_dispatcher, events}
    end)
    |> Enum.each(fn {partition_dispatcher, events} ->
      PartitionDispatcher.await(partition_dispatcher, events)
    end)

    version = events |> Enum.map(fn %{id: id} -> id end) |> Enum.max()
    reducer.set_version(version)

    {:noreply, %{state | version: version}}
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
