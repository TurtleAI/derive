defmodule Derive.Dispatcher do
  use GenServer

  alias Derive.PartitionDispatcher

  alias __MODULE__, as: S

  defstruct [:reducer, :batch_size, :partition]

  @type t :: %__MODULE__{
          reducer: Derive.Reducer.t(),
          batch_size: integer(),
          partition: Derive.Reducer.partition()
        }

  # We maintain the version of a special partition with this name
  @global_partition "$"

  @moduledoc """
  Responsible for keeping derived state up to date based on implementation `Derive.Reducer`

  Events are processed concurrently, but in order is guaranteed based on the result of Derive.Reducer.partition/1
  State is eventually consistent
  You can call `&Derive.Dispatcher.await/2` lets you wait for events to be finished processing.

  `Derive.Dispatcher` doesn't do the actual processing itself, it forwards events
  to processes defined by `Derive.PartitionDispatcher`
  """

  @spec start_link(Derive.Reducer.t(), any()) :: {:ok, pid()} | {:error, any()}
  def start_link(reducer, opts \\ []) do
    {dispatcher_opts, genserver_opts} = Keyword.split(opts, [:batch_size])

    batch_size = Keyword.get(dispatcher_opts, :batch_size, 100)

    GenServer.start_link(__MODULE__, %S{reducer: reducer, batch_size: batch_size}, genserver_opts)
  end

  @doc """
  Rebuilds the state of a reducer.
  This means the state will be reset and all of the events processed to get to the final state.
  """
  @spec rebuild(Derive.Reducer.t()) :: :ok
  def rebuild(reducer) do
    reducer.reset_state()

    {:ok, _dispatcher} = start_link(reducer)

    # @TODO: remove hack to get test passing
    # we really want to wait until all the events have been processed
    Process.sleep(500)
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

  @impl true
  def init(%S{reducer: reducer} = state) do
    Process.flag(:trap_exit, true)

    Derive.EventLog.subscribe(reducer.source(), self())

    # handle_continue(:load_partition...) will first boot with the version
    GenServer.cast(self(), :catchup_on_boot)

    {:ok, state, {:continue, :load_partition}}
  end

  ### Server

  @impl true
  def handle_continue(:load_partition, %S{reducer: reducer} = state) do
    partition = reducer.get_partition(@global_partition)
    {:noreply, %{state | partition: partition}}
  end

  @impl true
  def handle_call({:await, events}, _from, %S{reducer: reducer} = state) do
    List.wrap(events)
    |> events_by_partition_dispatcher(reducer)
    |> Enum.each(fn {partition_dispatcher, events} ->
      PartitionDispatcher.await(partition_dispatcher, events)
    end)

    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:new_events, _new_events}, state),
    do: {:noreply, catchup(state)}

  def handle_cast(:catchup_on_boot, state),
    do: {:noreply, catchup(state)}

  @impl true
  def handle_info({:EXIT, _, :normal}, state),
    do: {:stop, :shutdown, state}

  @impl true
  def terminate(_reason, _state),
    do: :ok

  defp catchup(
         %S{
           reducer: reducer,
           partition: %Derive.Partition{version: version} = partition,
           batch_size: batch_size
         } = state
       ) do
    case Derive.EventLog.fetch(reducer.source(), {version, batch_size}) do
      {[], _} ->
        # done processing so return the state as is
        state

      {events, new_version} ->
        events
        |> events_by_partition_dispatcher(reducer)
        |> Enum.map(fn {partition_dispatcher, events} ->
          PartitionDispatcher.dispatch_events(partition_dispatcher, events)
          {partition_dispatcher, events}
        end)
        |> Enum.each(fn {partition_dispatcher, events} ->
          PartitionDispatcher.await(partition_dispatcher, events)
        end)

        new_partition = %{partition | version: new_version}
        reducer.set_partition(new_partition)

        # we have more events left to process, so we recursively call catchup
        %{state | partition: new_partition}
        |> catchup()
    end
  end

  defp events_by_partition_dispatcher(events, reducer) do
    events_by_partition = Enum.group_by(events, &reducer.partition/1)

    for {partition, events} <- events_by_partition, partition != nil, into: %{} do
      partition_dispatcher = Derive.PartitionSupervisor.lookup_or_start({reducer, partition})
      {partition_dispatcher, events}
    end
  end
end
