defmodule Derive.PartitionDispatcher do
  @moduledoc """
  A process for a given {reducer, partition} to keep the state of its partition up to date

  processed_events:
    a set of event ids that have been processed so far
  pending_awaiters:
     [{awaiter, event_id}, {awaiter, event_id}, ...]
  """

  use GenServer, restart: :transient

  alias __MODULE__, as: S
  alias Derive.{Partition, Reducer}
  alias Derive.State.MultiOp

  defstruct [:reducer, :partition, :pending_awaiters]

  @type t :: %__MODULE__{
          reducer: Reducer.t(),
          partition: Partition.t(),
          pending_awaiters: any()
        }

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
  def dispatch_events(server, events),
    do: GenServer.cast(server, {:dispatch_events, events})

  @doc """
  Wait until all of the events are processed
  """
  def await(_server, []),
    do: :ok

  def await(server, [event | rest]) do
    GenServer.call(server, {:await, event})
    await(server, rest)
  end

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
  def handle_call(
        {:await, event},
        from,
        %S{pending_awaiters: pending_awaiters} = state
      ) do
    case processed_event?(state, event) do
      true ->
        # The event was already processed, so we can immediately reply :ok
        {:reply, :ok, state}

      false ->
        # The event hasn't yet been processed, so we hold onto a reference to the caller
        # At a later time, we will reply to these callers after we process the events
        new_state = %{
          state
          | pending_awaiters: [{from, event.id} | pending_awaiters]
        }

        {:noreply, new_state}
    end
  end

  @impl true
  def handle_cast({:dispatch_events, []}, state),
    do: {:noreply, state}

  def handle_cast(
        {:dispatch_events, events},
        %S{
          partition: %Partition{status: :error},
          pending_awaiters: pending_awaiters
        } = state
      ) do
    version = Enum.max_by(events, fn %{id: id} -> id end)
    # The awaiters that can be notified after these events get processed
    {awaiters_to_notify, pending_awaiters_left} =
      Enum.split_with(pending_awaiters, fn {_awaiter, event_id} ->
        version.version >= event_id
      end)

    Enum.each(awaiters_to_notify, fn {awaiter, _event_id} ->
      GenServer.reply(awaiter, :ok)
    end)

    new_state = %{
      state
      | pending_awaiters: pending_awaiters_left
    }

    {:noreply, new_state}
  end

  def handle_cast(
        {:dispatch_events, events},
        %S{
          reducer: reducer,
          partition: %Partition{} = partition,
          pending_awaiters: pending_awaiters
        } = state
      ) do
    multi = reducer.reduce_events(events, partition)

    multi =
      case multi do
        %MultiOp{status: :processed} -> reducer.commit(multi)
        multi -> multi
      end

    new_partition = multi.partition

    # The awaiters that can be notified after these events get processed
    {awaiters_to_notify, pending_awaiters_left} =
      Enum.split_with(pending_awaiters, fn {_awaiter, event_id} ->
        new_partition.version >= event_id
      end)

    Enum.each(awaiters_to_notify, fn {awaiter, _event_id} ->
      GenServer.reply(awaiter, :ok)
    end)

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

  # if there's an error we stop caring
  # everything is marked as processed
  defp processed_event?(%S{partition: %{status: :error}}, _) do
    true
  end

  defp processed_event?(%S{partition: %{version: version}}, %{id: id}),
    do: version >= id
end
