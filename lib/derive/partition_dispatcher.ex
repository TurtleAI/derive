defmodule Derive.PartitionDispatcher do
  use GenServer, restart: :temporary
  require Logger

  alias Derive.Util.MapOfSets
  alias __MODULE__, as: D

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
      D,
      %D{
        reducer: reducer,
        partition: partition,
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
    {:ok, state, {:continue, :ok}}
  end

  def handle_continue(:ok, %D{reducer: reducer, partition: partition} = state) do
    version = reducer.get_version(partition)
    {:noreply, %{state | version: version}}
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
        %D{pending_awaiters: pending_awaiters} = state
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

  def handle_cast(
        {:dispatch_events, events},
        %D{
          reducer: reducer,
          partition: partition,
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

    new_version = events |> Enum.map(fn %{id: id} -> id end) |> Enum.max()

    {matching_awaiters, pending_awaiters_left} =
      Enum.split_with(pending_awaiters, fn {_awaiter, event_id} ->
        new_version >= event_id
      end)

    Enum.each(matching_awaiters, fn {awaiter, _event_id} ->
      GenServer.reply(awaiter, :ok)
    end)

    new_state = %{
      state
      | pending_awaiters: pending_awaiters_left,
        version: new_version
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

  defp processed_event?(%D{version: version}, id) when is_binary(id),
    do: version >= id

  defp processed_event?(%D{version: version}, %{id: id}),
    do: version >= id
end
