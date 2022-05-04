defmodule Derive.PartitionDispatcher do
  @moduledoc """
  A process for a given {reducer, partition} to keep the state of its partition up to date
  """

  use GenServer, restart: :transient

  alias __MODULE__, as: S
  alias Derive.{Partition, Reducer}
  alias Derive.State.MultiOp

  require Logger

  @type t :: %__MODULE__{
          reducer: Reducer.t(),
          partition: Partition.t(),
          pending_awaiters: [pending_awaiter()]
        }

  defstruct [:reducer, :partition, :pending_awaiters]

  @typedoc """
  A process which has called await, along with a version it is waiting for
  Once we have processed up to or past the cursor the awaiter will be notified
  """
  @type pending_awaiter :: {GenServer.from(), Reducer.cursor()}

  def start_link(opts) do
    reducer = Keyword.fetch!(opts, :reducer)
    partition_id = Keyword.fetch!(opts, :partition)

    GenServer.start_link(
      __MODULE__,
      %S{
        reducer: reducer,
        partition: %Partition{id: partition_id},
        pending_awaiters: []
      },
      opts
    )
  end

  ### Client

  @doc """
  Asynchronously dispatch events to get processed and committed
  To wait for the events to get processed, use `&Derive.PartitionDispatcher.await/2`
  """
  @spec dispatch_events(pid(), [Derive.EventLog.event()], pid() | nil) :: :ok
  def dispatch_events(server, events, logger),
    do: GenServer.cast(server, {:dispatch_events, events, logger})

  @doc """
  Wait until an event has been processed
  If it has already been processed, this should complete immediately
  """
  @spec await(pid(), Derive.EventLog.event()) :: :ok
  def await(server, event),
    do: GenServer.call(server, {:await, event})

  @doc """
  Register an awaiter process that will be notified with `GenServer.reply(reply_to, :ok)`
  Once an event is processed
  If it has already been processed, this awaiter will be notified immediately
  """
  @spec register_awaiter(pid(), GenServer.from(), Derive.EventLog.event()) :: :ok
  def register_awaiter(server, reply_to, event),
    do: GenServer.cast(server, {:register_awaiter, reply_to, event})

  ### Server

  @impl true
  def init(state) do
    Process.flag(:trap_exit, true)
    {:ok, state, {:continue, :load_partition}}
  end

  @impl true
  def handle_continue(:load_partition, %S{reducer: reducer, partition: %{id: id}} = state) do
    partition = reducer.get_partition(id)
    {:noreply, %{state | partition: partition}}
  end

  @impl true
  def handle_info(:timeout, state),
    do: {:stop, :normal, state}

  def handle_info({:EXIT, _, :normal}, state),
    do: {:stop, :shutdown, state}

  @impl true
  def handle_call({:await, event}, from, state) do
    register_awaiter(self(), from, event)
    {:noreply, state}
  end

  @impl true
  def handle_cast(
        {:register_awaiter, reply_to, event},
        %S{
          reducer: reducer,
          partition: %{status: status} = partition,
          pending_awaiters: pending_awaiters
        } = state
      ) do
    # if there has been an error or if we've already processed the event,
    # await completes immediately
    case status == :error || reducer.processed_event?(partition, event) do
      true ->
        # The event was already processed, so we can immediately reply :ok
        GenServer.reply(reply_to, :ok)
        {:noreply, state}

      false ->
        # The event hasn't yet been processed, so we hold onto a reference to the caller
        # At a later time, we will reply to these callers after we process the events
        new_state = %{
          state
          | pending_awaiters: [{reply_to, event.id} | pending_awaiters]
        }

        {:noreply, new_state}
    end
  end

  def handle_cast({:dispatch_events, []}, state),
    do: {:noreply, state}

  def handle_cast(
        {:dispatch_events, events, logger},
        %S{
          reducer: reducer,
          partition: %Partition{} = partition,
          pending_awaiters: pending_awaiters
        } = state
      ) do
    multi = reducer.process_events(events, MultiOp.new(partition))

    # log out what happened
    case multi do
      %MultiOp{status: :committed} = multi ->
        Derive.Logger.committed(logger, multi)
        multi

      %MultiOp{status: :error, error: error} = multi ->
        Logger.error(inspect(error))
        multi

      _ ->
        :ok
    end

    new_partition = multi.partition

    # The awaiters that can be notified after these events get processed
    {awaiters_to_notify, pending_awaiters_left} =
      Enum.split_with(pending_awaiters, fn {_awaiter, event_id} ->
        new_partition.cursor >= event_id
      end)

    notify_awaiters(awaiters_to_notify)

    new_state = %{
      state
      | pending_awaiters: pending_awaiters_left,
        partition: new_partition
    }

    {:noreply, new_state}
  end

  @impl true
  def terminate(_, _state),
    do: :ok

  defp notify_awaiters(awaiters) do
    Enum.each(awaiters, fn {reply_to, _event_id} ->
      GenServer.reply(reply_to, :ok)
    end)
  end
end
