defmodule Derive.EventLog.InMemoryEventLog do
  use GenServer

  defstruct events: [], subscribers: []

  alias __MODULE__, as: S

  @moduledoc """
  An ephemeral in-memory event log used just for testing purposes.
  """

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, :ok, opts)

  ### Client

  @doc """
  Append a list of events to the event log
  """
  def append(server, events),
    do: GenServer.call(server, {:append, events})

  ### Server

  @impl true
  def init(:ok),
    do: {:ok, %S{}}

  @impl true
  def handle_call(
        {:append, new_events},
        _from,
        %S{subscribers: subscribers, events: events} = state
      ) do
    notify_subscribers(subscribers, {:new_events, new_events})
    {:reply, :ok, %{state | events: events ++ new_events}}
  end

  def handle_call({:subscribe, subscriber}, {pid, _}, state) do
    # If this subscriber process terminates, we want to monitor it and remove it
    # from the list of subscribers so we aren't sending messages to dead processes
    Process.monitor(pid)
    {:reply, :ok, add_subscriber(state, subscriber)}
  end

  def handle_call({:unsubscribe, subscriber}, _from, state) do
    {:reply, :ok, remove_subscriber(state, subscriber)}
  end

  def handle_call({:fetch, {cursor, limit}}, _from, %S{events: events} = state) do
    index = index_of_cursor(cursor, 0, events)

    case Enum.slice(events, index, limit) do
      [] ->
        {:reply, {[], cursor}, state}

      events_slice ->
        {:reply, {events_slice, Enum.at(events_slice, -1) |> Map.get(:id)}, state}
    end
  end

  @impl true
  def handle_info(:timeout, state),
    do: {:stop, :normal, state}

  def handle_info({:EXIT, _, :normal}, state),
    do: {:stop, :shutdown, state}

  def handle_info({:DOWN, _ref, :process, pid, _}, state),
    do: {:noreply, remove_subscriber(state, pid)}

  defp notify_subscribers([], _events),
    do: :ok

  defp notify_subscribers([subscriber | rest], message) do
    GenServer.cast(subscriber, message)
    notify_subscribers(rest, message)
  end

  defp add_subscriber(%S{subscribers: subscribers} = state, subscriber) do
    %{state | subscribers: subscribers ++ [subscriber]}
  end

  defp remove_subscriber(%S{subscribers: subscribers} = state, subscriber) do
    %{state | subscribers: Enum.reject(subscribers, &(&1 == subscriber))}
  end

  defp index_of_cursor(:start, _idx, _events),
    do: 0

  defp index_of_cursor(_, index, []),
    do: index

  # we want to exclude this element because it matches the id
  # so we have to skip to the next index
  defp index_of_cursor(cursor, index, [%{id: cursor} | _rest]),
    do: index + 1

  # the id is already greater than the cursor
  # so we start exactly at this index
  defp index_of_cursor(cursor, index, [%{id: id} | _rest]) when id > cursor,
    do: index

  defp index_of_cursor(cursor, index, [%{id: id} | rest]) when id < cursor,
    do: index_of_cursor(cursor, index + 1, rest)
end
