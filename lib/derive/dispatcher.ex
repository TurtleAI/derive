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

  alias Derive.{Options, EventBatch, Partition, PartitionDispatcher}

  alias __MODULE__, as: S

  @type t :: %__MODULE__{
          options: Derive.Options.t(),
          partition: Derive.Partition.t() | nil,
          status: status(),
          partition_supervisor: atom(),
          catchup_awaiters: [GenServer.from()]
        }

  @type status :: :booting | :processing | :caught_up

  defstruct [:partition, :status, :options, :partition_supervisor, :catchup_awaiters]

  @type server :: pid() | atom()

  @type option ::
          {:options, Derive.Options.t()}
          | {:partition_supervisor, atom()}
          | GenServer.option()

  @spec start_link([option]) :: {:ok, server()} | {:error, term()}
  def start_link(opts \\ []) do
    {dispatcher_opts, genserver_opts} = Keyword.split(opts, Map.keys(__struct__()))

    options = Keyword.fetch!(dispatcher_opts, :options)
    partition_supervisor = Keyword.fetch!(dispatcher_opts, :partition_supervisor)

    GenServer.start_link(
      __MODULE__,
      %S{
        status: :booting,
        options: options,
        partition_supervisor: partition_supervisor,
        catchup_awaiters: []
      },
      genserver_opts
    )
  end

  ### Server

  @impl true
  def init(
        %S{options: %Options{source: source, mode: mode, reducer: reducer, logger: logger}} =
          state
      ) do
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
  def handle_continue(:load_partition, %S{options: %Options{reducer: reducer} = options} = state) do
    partition = reducer.load_partition(options, Partition.global_id())

    GenServer.cast(self(), :catchup)

    loaded_state = %{state | partition: partition, status: :processing}

    {:noreply, loaded_state}
  end

  @impl true
  def handle_call(
        :await_catchup,
        _from,
        %S{status: :caught_up} = state
      ) do
    {:reply, :ok, state}
  end

  def handle_call(
        :await_catchup,
        from,
        %S{
          catchup_awaiters: catchup_awaiters
        } = state
      ) do
    {:noreply, %S{state | catchup_awaiters: [from | catchup_awaiters]}}
  end

  @impl true
  def handle_cast({:new_events, _new_events}, %S{} = state),
    do: handle_cast(:catchup, state)

  def handle_cast(
        :catchup,
        %S{options: %{mode: mode}} = state
      ) do
    case catchup(state) do
      {:continue, new_state} ->
        # recursively call catchup to process the next batch of events

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
           partition: %Derive.Partition{cursor: cursor} = partition,
           partition_supervisor: partition_supervisor,
           catchup_awaiters: catchup_awaiters,
           options:
             %Options{
               reducer: reducer,
               source: source,
               batch_size: batch_size,
               logger: logger
             } = options
         } = state
       ) do
    case Derive.EventLog.fetch(source, {cursor, batch_size}) do
      {[], new_cursor} ->
        # done processing all events
        Derive.Logger.log(logger, {:caught_up, reducer, %{partition | cursor: new_cursor}})

        reply_all(Enum.reverse(catchup_awaiters), :ok)

        {:done, %S{state | status: :caught_up, catchup_awaiters: []}}

      {events, new_cursor} ->
        events_by_partition_id =
          Enum.group_by(events, &reducer.partition/1)
          |> Enum.reject(fn {partition, _events} ->
            partition == nil
          end)

        events_by_partition_id_dispatcher =
          for {partition_id, events} <- events_by_partition_id, into: %{} do
            partition_dispatcher =
              Derive.PartitionSupervisor.start_child(
                partition_supervisor,
                {options, partition_id}
              )

            {partition_dispatcher, events}
          end

        Enum.each(events_by_partition_id_dispatcher, fn {partition_dispatcher, events} ->
          PartitionDispatcher.dispatch_events(partition_dispatcher, %EventBatch{
            events: events,
            logger: logger
          })
        end)

        # We want to wait until all of the partitions have processed the events
        # before updating the cursor of this partition

        partitions_with_messages =
          for {partition_dispatcher, events} <- events_by_partition_id_dispatcher, e <- events do
            {partition_dispatcher, {:await, e}}
          end

        Derive.Ext.GenServer.call_many(partitions_with_messages, 30_000)

        new_partition = %{partition | cursor: new_cursor}
        reducer.save_partition(options, new_partition)

        Derive.Logger.log(logger, {:events_processed, Enum.count(events)})

        # we have more events left to process
        {:continue, %{state | partition: new_partition, status: :processing}}
    end
  end

  defp reply_all([], _message),
    do: :ok

  defp reply_all([awaiter | rest], message) do
    GenServer.reply(awaiter, message)
    reply_all(rest, message)
  end
end
