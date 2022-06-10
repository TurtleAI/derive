defmodule Derive.PartitionDispatcher do
  @moduledoc """
  A process for a given {reducer, partition} to keep the state of its partition up to date

  A `Derive.Dispatcher` doesn't do any processing of its own. It will lazily instantiate processes
  of `Derive.PartitionDispatcher` and forward events to each partition to do the actual processing.
  """

  use GenServer, restart: :transient

  alias __MODULE__, as: S
  alias Derive.{Partition, Reducer, EventBatch, Options, MultiOp}

  @type t :: %__MODULE__{
          options: Options.t(),
          partition: Partition.t(),
          awaiters: [awaiter()],
          timeout: timeout()
        }

  defstruct [:options, :partition, :awaiters, :timeout]

  @typedoc """
  A process which has called await, along with a version it is waiting for
  Once we have processed up to or past the target cursor the awaiter will be notified
  """
  @type awaiter :: {GenServer.from(), Reducer.cursor()}

  @default_timeout 30_000

  def start_link(opts) do
    options = Keyword.fetch!(opts, :options)
    partition_id = Keyword.fetch!(opts, :partition)
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    GenServer.start_link(
      __MODULE__,
      %S{
        options: options,
        partition: %Partition{id: partition_id},
        awaiters: [],
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
        %S{options: %Options{reducer: reducer} = options, partition: %{id: id}, timeout: timeout} =
          state
      ) do
    partition = reducer.load_partition(options, id)
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
          options: %Options{reducer: reducer},
          partition: %Partition{cursor: cursor, status: status, error: error},
          awaiters: awaiters,
          timeout: timeout
        } = state
      ) do
    event_cursor = reducer.get_cursor(event)

    cond do
      # have already processed past this event, so we can reply immediately
      cursor >= event_cursor ->
        GenServer.reply(reply_to, :ok)
        {:noreply, state, timeout}

      # This partition has errored out, so we want to immediately notify that this is an error
      status == :error ->
        GenServer.reply(reply_to, {:error, error})
        {:noreply, state, timeout}

      true ->
        # The event hasn't yet been processed, so we hold onto a reference to the caller
        # We will reply to this awaiter later on after we process the event
        new_state = %{
          state
          | awaiters: [{reply_to, event_cursor} | awaiters]
        }

        {:noreply, new_state, timeout}
    end
  end

  def handle_cast({:dispatch_events, %EventBatch{events: []}}, %S{timeout: timeout} = state),
    do: {:noreply, state, timeout}

  def handle_cast(
        {:dispatch_events, %EventBatch{events: events, logger: logger}},
        %S{
          options: %Options{reducer: reducer, logger: logger} = opts,
          partition: %Partition{} = partition,
          awaiters: awaiters,
          timeout: timeout
        } = state
      ) do
    multi = reducer.process_events(events, MultiOp.new(partition))

    Derive.Logger.multi(logger, multi)
    new_partition = multi.partition

    case multi do
      # we must save the partition because this implementation doesn't include saving
      # the partition as part of the commit step
      %MultiOp{save_partition: nil} -> reducer.save_partition(opts, new_partition)
      # We must save the partition because, even if we have a save_partition included in the commit
      # We've never got to this point because either we got stuck at handle_event or commit
      %MultiOp{status: :error} -> reducer.save_partition(opts, new_partition)
      _ -> :ok
    end

    # The awaiters that can be notified after these events get processed
    {{awaiters_to_notify, message}, awaiters_left} = split_awaiters(multi, awaiters)

    notify_awaiters(awaiters_to_notify, message)

    new_state = %{
      state
      | awaiters: awaiters_left,
        partition: new_partition
    }

    {:noreply, new_state, timeout}
  end

  @impl true
  def terminate(_, _state),
    do: :ok

  defp split_awaiters(%MultiOp{status: :error}, awaiters) do
    # if there's an error, we want to notify all awaiters that we're done
    {{awaiters, :error}, []}
  end

  defp split_awaiters(%MultiOp{partition: %Partition{status: :error}}, awaiters) do
    # if there's an error, we want to notify all awaiters that we're done
    {{awaiters, :error}, []}
  end

  defp split_awaiters(%MultiOp{partition: %Partition{cursor: partition_cursor}}, awaiters) do
    {awaiters_to_notify, awaiters_left} =
      Enum.split_with(awaiters, fn {_awaiter, target_cursor} ->
        partition_cursor >= target_cursor
      end)

    {{awaiters_to_notify, :ok}, awaiters_left}
  end

  defp notify_awaiters(awaiters, message) do
    Enum.each(awaiters, fn {reply_to, _event_id} ->
      GenServer.reply(reply_to, message)
    end)
  end
end
