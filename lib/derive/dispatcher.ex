defmodule Derive.Dispatcher do
  @moduledoc """
  Responsible for keeping derived state up to date based on implementation `Derive.Reducer`

  Events are processed concurrently, but in order is guaranteed based on the result of Derive.Reducer.partition/1
  State is eventually consistent
  You can call `&Derive.Dispatcher.await/2` lets you wait for events to be finished processing.

  `Derive.Dispatcher` doesn't do the actual processing itself, it forwards events
  to processes defined by `Derive.PartitionDispatcher`
  """

  use GenServer, restart: :transient

  alias Derive.PartitionDispatcher

  alias __MODULE__, as: S

  @type t :: %__MODULE__{
          reducer: Derive.Reducer.t(),
          batch_size: non_neg_integer(),
          partition: Derive.Reducer.partition(),
          lookup_or_start: function(),
          mode: mode(),
          logger: pid() | nil
        }

  defstruct [:reducer, :batch_size, :partition, :source, :lookup_or_start, :mode, :logger]

  @typedoc """
  The mode in which the dispatcher should operate

  catchup: on boot, catch up to the last point possible, stay alive and up to date
  rebuild: destroy the state, rebuild it up to the last point possible, then shut down
  """
  @type mode :: :catchup | :rebuild

  # We maintain the version of a special partition with this name
  @global_partition "$"

  @spec start_link(any()) :: {:ok, pid()} | {:error, any()}
  def start_link(opts \\ []) do
    {dispatcher_opts, genserver_opts} = Keyword.split(opts, Map.keys(__struct__()))

    reducer = Keyword.fetch!(dispatcher_opts, :reducer)
    batch_size = Keyword.get(dispatcher_opts, :batch_size, 100)
    source = Keyword.fetch!(dispatcher_opts, :source)
    lookup_or_start = Keyword.fetch!(dispatcher_opts, :lookup_or_start)
    mode = Keyword.get(dispatcher_opts, :mode, :catchup)
    logger = Keyword.get(dispatcher_opts, :logger)

    GenServer.start_link(
      __MODULE__,
      %S{
        reducer: reducer,
        batch_size: batch_size,
        source: source,
        lookup_or_start: lookup_or_start,
        mode: mode,
        logger: logger
      },
      genserver_opts
    )
  end

  ### Server

  @impl true
  def init(%S{source: source, mode: mode, reducer: reducer} = state) do
    Process.flag(:trap_exit, true)

    case mode do
      :catchup ->
        # only catchup needs to subscribe to new events
        Derive.EventLog.subscribe(source, self())

      :rebuild ->
        # reset the state before anything is loaded
        reducer.reset_state()
    end

    {:ok, state, {:continue, :load_partition}}
  end

  @impl true
  def handle_continue(:load_partition, %S{reducer: reducer} = state) do
    partition = reducer.get_partition(@global_partition)

    GenServer.cast(self(), :catchup_on_boot)

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
  def handle_cast({:new_events, _new_events}, %S{} = state) do
    {:noreply, catchup(state)}
  end

  def handle_cast(:catchup_on_boot, %S{mode: mode} = state) do
    new_state = catchup(state)

    case mode do
      :catchup -> {:noreply, new_state}
      :rebuild -> {:stop, :normal, new_state}
    end
  end

  @impl true
  def handle_info({:EXIT, _from, :normal}, state) do
    {:stop, :normal, state}
  end

  @impl true
  def terminate(_reason, _state),
    do: :ok

  defp catchup(
         %S{
           reducer: reducer,
           source: source,
           partition: %Derive.Partition{version: version} = partition,
           batch_size: batch_size,
           lookup_or_start: lookup_or_start,
           logger: logger
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
          PartitionDispatcher.dispatch_events(partition_dispatcher, events, logger)
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
