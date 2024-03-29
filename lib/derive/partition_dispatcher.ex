defmodule Derive.PartitionDispatcher do
  @moduledoc """
  A process for a given {reducer, partition} to keep the state of its partition up to date

  A `Derive.Dispatcher` doesn't do any processing of its own. It will lazily instantiate processes
  of `Derive.PartitionDispatcher` and forward events to each partition to do the actual processing.
  """

  use GenServer, restart: :transient

  require Logger

  alias __MODULE__, as: S
  alias Derive.{Partition, Reducer, Options, MultiOp}

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
  @spec dispatch_events(pid(), Derive.EventLog.event()) :: :ok
  def dispatch_events(server, event_batch),
    do: GenServer.cast(server, {:dispatch_events, event_batch})

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

  # handles :ok which might be produced for `&Derive.await/2`
  def handle_info({[:alias | _], :ok}, %S{timeout: timeout} = state) do
    {:noreply, state, timeout}
  end

  def handle_info(message, %S{timeout: timeout} = state) do
    Logger.warn("PartitionDispatcher handle_info unhandled: " <> inspect(message))
    {:noreply, state, timeout}
  end

  @impl true
  def handle_call(
        {:await, event},
        from,
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
        {:reply, :ok, state, timeout}

      # This partition has errored out, so we want to immediately notify that this is an error
      status == :error ->
        {:reply, {:error, error}, state, timeout}

      true ->
        # The event hasn't yet been processed, so we hold onto a reference to the caller
        # We will reply to this awaiter later on after we process the event
        new_state = %{
          state
          | awaiters: [{from, event_cursor} | awaiters]
        }

        {:noreply, new_state, timeout}
    end
  end

  @impl true
  def handle_cast({:dispatch_events, []}, %S{timeout: timeout} = state),
    do: {:noreply, state, timeout}

  def handle_cast(
        {:dispatch_events, events},
        %S{
          options: %Options{reducer: reducer, logger: logger} = opts,
          partition: %Partition{} = partition,
          awaiters: awaiters,
          timeout: timeout
        } = state
      ) do
    multi = reducer.process_events(events, MultiOp.new(partition), opts)

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

  defp split_awaiters(%MultiOp{status: :error, error: error}, awaiters) do
    # if there's an error, we want to notify all awaiters that we're done
    {{awaiters, {:error, error}}, []}
  end

  defp split_awaiters(%MultiOp{partition: %Partition{status: :error, error: error}}, awaiters) do
    # if there's an error, we want to notify all awaiters that we're done
    {{awaiters, {:error, error}}, []}
  end

  defp split_awaiters(
         %MultiOp{status: status, error: error, partition: %Partition{cursor: partition_cursor}},
         awaiters
       ) do
    {awaiters_to_notify, awaiters_left} =
      Enum.split_with(awaiters, fn {_awaiter, target_cursor} ->
        partition_cursor >= target_cursor
      end)

    reply =
      case status do
        :error ->
          {:error, error}

        _ ->
          :ok
      end

    {{awaiters_to_notify, reply}, awaiters_left}
  end

  defp notify_awaiters(awaiters, message) do
    Enum.each(awaiters, fn {reply_to, _event_id} ->
      GenServer.reply(reply_to, message)
    end)
  end
end
