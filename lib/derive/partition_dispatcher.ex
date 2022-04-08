defmodule Derive.PartitionDispatcher do
  use GenServer, restart: :temporary
  require Logger

  alias Derive.Util.MapOfSets

  defstruct [:reducer, :partition, :processed_events, :pending_awaiters]

  @moduledoc """
  A process for a given {reducer, partition} to keep the state of its partition up to date

  processed_events:
    a set of event ids that have been processed so far
  pending_awaiters:
     a map of sets
     key: event id
     value: a set of processes that have called await to wait until the event has been processed
  """

  def start_link(opts) do
    reducer = Keyword.fetch!(opts, :reducer)
    partition = Keyword.fetch!(opts, :partition)

    GenServer.start_link(
      __MODULE__,
      %__MODULE__{
        reducer: reducer,
        partition: partition,
        processed_events: MapSet.new(),
        pending_awaiters: MapOfSets.new()
      },
      opts
    )
  end

  ### Client

  @doc """
  Asynchronously dispatch to get processed and committed
  Completes immediately
  To wait for the events to get processed, use `&Derive.PartitionDispatcher.await/2`
  """
  def dispatch_events(server, events),
    do: GenServer.cast(server, {:dispatch_events, events})

  def await(_server, []), do: :ok

  def await(server, [event | rest]) do
    GenServer.call(server, {:await, event})
    await(server, rest)
  end

  ### Server

  def init(state) do
    Process.flag(:trap_exit, true)
    {:ok, state}
  end

  def handle_info(:timeout, state) do
    debug(fn -> "Terminating due to inactivity" end)
    {:stop, :normal, state}
  end

  def handle_info({:EXIT, _, :normal}, state) do
    debug(fn -> ":EXIT" end)
    {:stop, :shutdown, state}
  end

  def handle_call(
        {:await, event},
        from,
        %__MODULE__{
          processed_events: processed_events,
          pending_awaiters: pending_awaiters
        } = state
      ) do
    case MapSet.member?(processed_events, event) do
      true ->
        # The event was already processed, so we can immediately reply :ok
        {:reply, :ok, state}

      false ->
        # The event hasn't yet been processed, so we hold onto a reference to the caller
        # At a later time, we will reply to these callers after we process the events
        new_state = %{state | pending_awaiters: MapOfSets.put(pending_awaiters, event, from)}

        {:noreply, new_state}
    end
  end

  def handle_cast(
        {:dispatch_events, events},
        %__MODULE__{
          reducer: reducer,
          partition: partition,
          processed_events: processed_events,
          pending_awaiters: pending_awaiters
        } = state
      ) do
    event_operations =
      events
      |> Enum.map(fn e ->
        {e, List.wrap(reducer.handle_event(e))}
      end)
      |> Enum.reject(fn {_e, ops} -> ops == [] end)

    multi_op = Derive.State.MultiOp.new(partition, event_operations)

    reducer.commit_operations(multi_op)

    {matching_awaiters, pending_awaiters_left} = pop_matching_awaiters(events, pending_awaiters)

    Enum.each(matching_awaiters, &GenServer.reply(&1, :ok))

    new_state = %{
      state
      | processed_events: MapSet.union(processed_events, MapSet.new(events)),
        pending_awaiters: pending_awaiters_left
    }

    {:noreply, new_state}
  end

  def terminate(_, _state) do
    debug(fn -> "terminate" end)
    :ok
  end

  defp debug(message) do
    Logger.debug(inspect(self()) <> ": " <> message.())
  end

  # For a given set of events and the current processes that are still waiting on events to be processed
  # Give {matching_awaiters, remaining_pending_awaiters}
  defp pop_matching_awaiters(events, pending_awaiters) do
    Enum.reduce(events, {[], pending_awaiters}, fn e, {matching_awaiters_acc, awaiters_acc} ->
      case pending_awaiters do
        # There are processes waiting for this event go get processed
        %{^e => awaiters} ->
          {matching_awaiters_acc ++ awaiters, Map.delete(awaiters_acc, e)}

        %{} ->
          {matching_awaiters_acc, awaiters_acc}
      end
    end)
  end
end
