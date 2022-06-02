defmodule Derive.PartitionDispatcher do
  @moduledoc """
  A process for a given {reducer, partition} to keep the state of its partition up to date
  """

  use GenServer, restart: :transient

  alias __MODULE__, as: S
  alias Derive.{Partition, Reducer, EventBatch}
  alias Derive.State.MultiOp

  @type t :: %__MODULE__{
          reducer: Reducer.t(),
          partition: Partition.t(),
          pending_awaiters: [pending_awaiter()],
          timeout: timeout()
        }

  defstruct [:reducer, :partition, :pending_awaiters, :timeout]

  @typedoc """
  A process which has called await, along with a version it is waiting for
  Once we have processed up to or past the target cursor the awaiter will be notified
  """
  @type pending_awaiter :: {GenServer.from(), Reducer.cursor()}

  @default_timeout 30_000

  def start_link(opts) do
    reducer = Keyword.fetch!(opts, :reducer)
    partition_id = Keyword.fetch!(opts, :partition)
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    GenServer.start_link(
      __MODULE__,
      %S{
        reducer: reducer,
        partition: %Partition{id: partition_id},
        pending_awaiters: [],
        timeout: timeout
      },
      opts
    )
  end

  ### Client

  @doc """
  Asynchronously dispatch events to get processed and committed
  To wait for the events to get processed, use `&Derive.PartitionDispatcher.await/2`
  """
  @spec dispatch_events(pid(), EventBatch.t()) :: :ok
  def dispatch_events(server, event_batch),
    do: GenServer.cast(server, {:dispatch_events, event_batch})

  @doc """
  Wait until an event has been processed
  If it has already been processed, this should complete immediately
  """
  @spec await(pid(), Derive.EventLog.event()) :: :ok
  def await(server, event),
    do: GenServer.call(server, {:await, event}, 30_000)

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
  def init(%S{} = state) do
    Process.flag(:trap_exit, true)
    {:ok, state, {:continue, :load_partition}}
  end

  @impl true
  def handle_continue(
        :load_partition,
        %S{reducer: reducer, partition: %{id: id}, timeout: timeout} = state
      ) do
    partition = reducer.load_partition(id)
    new_state = %{state | partition: partition}
    # Logger.info("BOOT " <> Partition.to_string(partition))
    {:noreply, new_state, timeout}
  end

  @impl true
  def handle_info(:timeout, %S{} = state) do
    # Logger.info("#{reducer}: SHUT DOWN " <> inspect(partition))
    {:stop, :normal, state}
  end

  def handle_info({:EXIT, _, :normal}, state),
    do: {:stop, :shutdown, state}

  @impl true
  def handle_call({:await, event}, from, %S{timeout: timeout} = state) do
    register_awaiter(self(), from, event)
    {:noreply, state, timeout}
  end

  @impl true
  def handle_cast(
        {:register_awaiter, reply_to, event},
        %S{
          reducer: reducer,
          partition: %Partition{cursor: cursor},
          pending_awaiters: pending_awaiters,
          timeout: timeout
        } = state
      ) do
    event_cursor = reducer.get_cursor(event)
    # if there has been an error or if we've already processed the event,
    # await completes immediately
    case cursor >= event_cursor do
      true ->
        # The event was already processed, so we can immediately reply :ok
        GenServer.reply(reply_to, :ok)
        {:noreply, state, timeout}

      false ->
        # The event hasn't yet been processed, so we hold onto a reference to the caller
        # At a later time, we will reply to these callers after we process the events
        new_state = %{
          state
          | pending_awaiters: [{reply_to, event_cursor} | pending_awaiters]
        }

        {:noreply, new_state, timeout}
    end
  end

  def handle_cast({:dispatch_events, %EventBatch{events: []}}, %S{timeout: timeout} = state),
    do: {:noreply, state, timeout}

  def handle_cast(
        {:dispatch_events, %EventBatch{events: events, logger: logger}},
        %S{
          reducer: reducer,
          partition: %Partition{} = partition,
          pending_awaiters: pending_awaiters,
          timeout: timeout
        } = state
      ) do
    multi = reducer.process_events(events, MultiOp.new(partition))

    log_multi(state, logger, multi)
    new_partition = multi.partition

    # The awaiters that can be notified after these events get processed
    {awaiters_to_notify, pending_awaiters_left} =
      Enum.split_with(pending_awaiters, fn {_awaiter, target_cursor} ->
        new_partition.cursor >= target_cursor
      end)

    notify_awaiters(awaiters_to_notify)

    new_state = %{
      state
      | pending_awaiters: pending_awaiters_left,
        partition: new_partition
    }

    {:noreply, new_state, timeout}
  end

  @impl true
  def terminate(_, _state),
    do: :ok

  defp log_multi(_, logger, %MultiOp{status: :committed} = multi) do
    Derive.Logger.committed(logger, multi)
    multi
  end

  defp log_multi(
         %S{} = _state,
         logger,
         %MultiOp{status: :error} = multi
       ) do
    Derive.Logger.log(
      logger,
      {:error, {:multi_op, multi}}
    )
  end

  defp notify_awaiters(awaiters) do
    Enum.each(awaiters, fn {reply_to, _event_id} ->
      GenServer.reply(reply_to, :ok)
    end)
  end
end
