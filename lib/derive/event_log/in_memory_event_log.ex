defmodule Derive.EventLog.InMemoryEventLog do
  use GenServer

  defstruct events: [], subscribers: []

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
    do: {:ok, %__MODULE__{}}

  @impl true
  def handle_call(
        {:append, new_events},
        _from,
        %{subscribers: subscribers, events: events} = state
      ) do
    notify_subscribers(subscribers, new_events)
    {:reply, :ok, %{state | events: events ++ new_events}}
  end

  def handle_call({:subscribe, new_subscriber}, _from, %{subscribers: subscribers} = state) do
    {:reply, :ok, %{state | subscribers: subscribers ++ [new_subscriber]}}
  end

  def handle_call({:fetch, {cursor, limit}}, _from, %{events: events} = state) do
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

  defp notify_subscribers([], _events),
    do: :ok

  defp notify_subscribers([subscriber | rest], events) do
    GenServer.cast(subscriber, {:new_events, events})
    notify_subscribers(rest, events)
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
