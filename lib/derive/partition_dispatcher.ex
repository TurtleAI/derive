defmodule Derive.PartitionDispatcher do
  use GenServer, restart: :temporary
  require Logger

  alias Derive.Util.MapOfSets

  defstruct [:reducer, :processed_events, :pending_awaiters]

  @moduledoc """
  A process for a given {reducer, partition} to keep the state of its partition up to date
  """

  def start_link(opts) do
    reducer = Keyword.fetch!(opts, :reducer)

    GenServer.start_link(
      __MODULE__,
      %__MODULE__{
        reducer: reducer,
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
        %{processed_events: processed_events, pending_awaiters: pending_awaiters} = state
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
        %{
          reducer: reducer,
          processed_events: processed_events,
          pending_awaiters: pending_awaiters
        } = state
      ) do
    events
    |> Enum.map(&reducer.handle_event/1)
    |> reducer.commit_operations()

    # for the given events that were just dispatched
    # - build a list of awaiters that should be notified
    # - build a new map of awaiters with the ones we're going to notify removed
    {matching_awaiters, new_pending_awaiters} =
      Enum.reduce(events, {[], pending_awaiters}, fn e, {matching_awaiters_acc, awaiters_acc} ->
        case pending_awaiters do
          %{^e => awaiters} ->
            {matching_awaiters_acc ++ awaiters, Map.delete(awaiters_acc, e)}

          %{} ->
            {matching_awaiters_acc, awaiters_acc}
        end
      end)

    Enum.each(matching_awaiters, fn awaiter ->
      GenServer.reply(awaiter, :ok)
    end)

    new_state = %{
      state
      | processed_events: MapSet.union(processed_events, MapSet.new(events)),
        pending_awaiters: new_pending_awaiters
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
end
