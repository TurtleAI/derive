defmodule Derive.PartitionDispatcher do
  use GenServer, restart: :temporary
  require Logger

  alias __MODULE__, as: S
  alias Derive.State.MultiOp

  defstruct [:reducer, :partition, :version, :pending_awaiters]

  @moduledoc """
  A process for a given {reducer, partition} to keep the state of its partition up to date

  processed_events:
    a set of event ids that have been processed so far
  pending_awaiters:
     [{awaiter, event_id}, {awaiter, event_id}, ...]
  """

  def start_link(opts) do
    reducer = Keyword.fetch!(opts, :reducer)
    partition = Keyword.fetch!(opts, :partition)

    GenServer.start_link(
      __MODULE__,
      %S{
        reducer: reducer,
        partition: partition,
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

  def init(state) do
    Process.flag(:trap_exit, true)
    {:ok, state, {:continue, :load_version}}
  end

  def handle_continue(:load_version, %S{reducer: reducer, partition: partition} = state) do
    version = reducer.get_version(partition)
    {:noreply, %{state | version: version}}
  end

  def handle_info(:timeout, state),
    do: {:stop, :normal, state}

  def handle_info({:EXIT, _, :normal}, state),
    do: {:stop, :shutdown, state}

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

  def handle_cast({:dispatch_events, []}, state),
    do: {:noreply, state}

  def handle_cast(
        {:dispatch_events, events},
        %S{
          reducer: reducer,
          partition: partition,
          version: version,
          pending_awaiters: pending_awaiters
        } = state
      ) do
    multi_op =
      Derive.Util.handle_events(
        events,
        reducer,
        partition
      )

    reducer.commit_operations(multi_op)

    new_version = max(MultiOp.partition_version(multi_op), version)

    # The awaiters that can be notified after these events get processed
    {awaiters_to_notify, pending_awaiters_left} =
      Enum.split_with(pending_awaiters, fn {_awaiter, event_id} ->
        new_version >= event_id
      end)

    Enum.each(awaiters_to_notify, fn {awaiter, _event_id} ->
      GenServer.reply(awaiter, :ok)
    end)

    new_state = %{
      state
      | pending_awaiters: pending_awaiters_left,
        version: new_version
    }

    {:noreply, new_state}
  end

  def terminate(_, _state),
    do: :ok

  defp processed_event?(%S{version: version}, id) when is_binary(id),
    do: version >= id

  defp processed_event?(%S{version: version}, %{id: id}),
    do: version >= id
end
