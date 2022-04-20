defmodule Derive.Dispatcher do
  use GenServer

  alias Derive.PartitionDispatcher

  alias __MODULE__, as: S

  defstruct [:reducer, :batch_size, :partition, :source, :lookup_or_start]

  @type t :: %__MODULE__{
          reducer: Derive.Reducer.t(),
          batch_size: integer(),
          partition: Derive.Reducer.partition(),
          lookup_or_start: function()
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

  @spec start_link(any()) :: {:ok, pid()} | {:error, any()}
  def start_link(opts \\ []) do
    {dispatcher_opts, genserver_opts} =
      Keyword.split(opts, [:reducer, :batch_size, :source, :lookup_or_start])

    reducer = Keyword.fetch!(dispatcher_opts, :reducer)
    batch_size = Keyword.fetch!(dispatcher_opts, :batch_size)
    source = Keyword.fetch!(dispatcher_opts, :source)
    lookup_or_start = Keyword.fetch!(dispatcher_opts, :lookup_or_start)

    GenServer.start_link(
      __MODULE__,
      %S{
        reducer: reducer,
        batch_size: batch_size,
        source: source,
        lookup_or_start: lookup_or_start
      },
      genserver_opts
    )
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

  ### Server

  @impl true
  def init(%S{source: source} = state) do
    Process.flag(:trap_exit, true)

    Derive.EventLog.subscribe(source, self())

    # handle_continue(:load_partition...) will first boot with the version
    GenServer.cast(self(), :catchup_on_boot)

    {:ok, state, {:continue, :load_partition}}
  end

  @impl true
  def handle_continue(:load_partition, %S{reducer: reducer} = state) do
    partition = reducer.get_partition(@global_partition)
    {:noreply, %{state | partition: partition}}
  end

  @impl true
  def handle_call(
        {:await, events},
        _from,
        %S{reducer: reducer, lookup_or_start: lookup_or_start} = state
      ) do
    List.wrap(events)
    |> events_by_partition_dispatcher(reducer, lookup_or_start)
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
           source: source,
           partition: %Derive.Partition{version: version} = partition,
           batch_size: batch_size,
           lookup_or_start: lookup_or_start
         } = state
       ) do
    case Derive.EventLog.fetch(source, {version, batch_size}) do
      {[], _} ->
        # done processing so return the state as is
        state

      {events, new_version} ->
        events
        |> events_by_partition_dispatcher(reducer, lookup_or_start)
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

  defp events_by_partition_dispatcher(events, reducer, lookup_or_start) do
    events_by_partition = Enum.group_by(events, &reducer.partition/1)

    for {partition, events} <- events_by_partition, partition != nil, into: %{} do
      partition_dispatcher = lookup_or_start.({reducer, partition})
      {partition_dispatcher, events}
    end
  end
end
