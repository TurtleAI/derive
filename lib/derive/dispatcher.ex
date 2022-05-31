defmodule Derive.Dispatcher do
  @moduledoc """
  Responsible for keeping derived state up to date based on implementation `Derive.Reducer`

  Events are processed concurrently, but in order is guaranteed based on the result of Derive.Reducer.partition/1
  State is eventually consistent
  You can call `&Derive.Dispatcher.await/2` lets you wait for events to be finished processing.

  `Derive.Dispatcher` doesn't do any processing itself, it forwards events
  to processes defined by `Derive.PartitionDispatcher`
  """

  use GenServer, restart: :transient

  alias Derive.{Partition, PartitionDispatcher, Reducer}

  alias __MODULE__, as: S

  @type t :: %__MODULE__{
          reducer: Reducer.t(),
          batch_size: non_neg_integer(),
          partition: Reducer.partition(),
          lookup_or_start: PartitionDispatcher.lookup_or_start(),
          mode: mode(),
          logger: Derive.Logger.t()
        }

  defstruct [:reducer, :batch_size, :partition, :source, :lookup_or_start, :mode, :logger]

  @typedoc """
  The mode in which the dispatcher should operate

  catchup: on boot, catch up to the last point possible, stay alive and up to date
  rebuild: destroy the state, rebuild it up to the last point possible, then shut down
  """
  @type mode :: :catchup | :rebuild

  @type server :: pid() | atom()

  @type dispatcher_option ::
          {:reducer, Reducer.t()}
          | {:batch_size, non_neg_integer()}
          | {:partition, Reducer.partition()}
          | {:lookup_or_start, PartitionDispatcher.lookup_or_start()}
          | {:mode, mode()}
          | {:logger, Derive.Logger.t()}

  @type option :: dispatcher_option() | GenServer.option()

  @spec start_link([option]) :: {:ok, server()} | {:error, term()}
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

  ### Client
  @spec await(server(), [Derive.EventLog.event()]) :: :ok
  def await(_server, []),
    do: :ok

  def await(server, [event | rest]) do
    GenServer.call(server, {:await, event}, 30_000)
    await(server, rest)
  end

  ### Server

  @impl true
  def init(%S{source: source, mode: mode, reducer: reducer, logger: logger} = state) do
    Process.flag(:trap_exit, true)

    case mode do
      :catchup ->
        # only catchup needs to subscribe to new events
        Derive.EventLog.subscribe(source, self())

      :rebuild ->
        count = Derive.EventLog.count(source)
        Derive.Logger.log(logger, {:rebuild_started, count})

        # reset the state before anything is loaded
        reducer.reset_state()
    end

    {:ok, state, {:continue, :load_partition}}
  end

  @impl true
  def handle_continue(:load_partition, %S{reducer: reducer} = state) do
    partition = reducer.load_partition(Partition.global_id())

    GenServer.cast(self(), :catchup)

    loaded_state = %{state | partition: partition}

    {:noreply, loaded_state}
  end

  @impl true
  def handle_call(
        {:await, event},
        from,
        %S{reducer: reducer, lookup_or_start: lookup_or_start} = state
      ) do
    # if a partition goes to nil, we consider it processed since it'll never get processed
    case reducer.partition(event) do
      nil ->
        {:reply, :ok, state}

      partition ->
        partition_dispatcher = lookup_or_start.({reducer, partition})
        PartitionDispatcher.register_awaiter(partition_dispatcher, from, event)
        {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:new_events, _new_events}, %S{} = state),
    do: handle_cast(:catchup, state)

  def handle_cast(:catchup, %S{mode: mode} = state) do
    case catchup(state) do
      {:continue, new_state} ->
        # recursively call catchup

        GenServer.cast(self(), :catchup)
        {:noreply, new_state}

      {:done, new_state} ->
        case mode do
          :catchup -> {:noreply, new_state}
          :rebuild -> {:stop, :normal, new_state}
        end
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
           partition: %Derive.Partition{cursor: cursor} = partition,
           batch_size: batch_size,
           lookup_or_start: lookup_or_start,
           logger: logger
         } = state
       ) do
    case Derive.EventLog.fetch(source, {cursor, batch_size}) do
      {[], new_cursor} ->
        # done processing all events
        Derive.Logger.log(logger, {:caught_up, reducer, %{partition | cursor: new_cursor}})
        {:done, state}

      {events, new_cursor} ->
        events_by_partition =
          Enum.group_by(events, &reducer.partition/1)
          |> Enum.reject(fn {partition, _events} ->
            partition == nil
          end)

        events_by_partition_dispatcher =
          for {partition, events} <- events_by_partition, into: %{} do
            partition_dispatcher = lookup_or_start.({reducer, partition})
            {partition_dispatcher, events}
          end

        Enum.each(events_by_partition_dispatcher, fn {partition_dispatcher, events} ->
          PartitionDispatcher.dispatch_events(partition_dispatcher, events, logger)
        end)

        # We want to wait until all of the partitions have processed the events
        # before updating the cursor of this partition
        for {partition_dispatcher, events} <- events_by_partition_dispatcher, e <- events do
          PartitionDispatcher.await(partition_dispatcher, e)
        end

        new_partition = %{partition | cursor: new_cursor}
        reducer.save_partition(new_partition)

        Derive.Logger.log(logger, {:events_processed, Enum.count(events)})

        # we have more events left to process
        {:continue, %{state | partition: new_partition}}
    end
  end
end
