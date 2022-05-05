defmodule Derive.EventLog.InMemoryEventLog do
  @moduledoc """
  An ephemeral in-memory implementation of `Derive.EventLog`
  Currently only meant for testing purposes
  """

  use GenServer

  alias Derive.Broadcaster

  @type t :: %__MODULE__{
          broadcaster: Broadcaster.server(),
          events: Derive.EventLog.event()
        }
  defstruct [:broadcaster, events: []]

  alias __MODULE__, as: S

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, %S{}, opts)

  ### Client

  @doc """
  Append a list of events to the event log
  """
  def append(server, events),
    do: GenServer.call(server, {:append, events})

  ### Server

  @impl true
  def init(state) do
    {:ok, broadcaster} = Broadcaster.start_link()
    {:ok, %{state | broadcaster: broadcaster}}
  end

  @impl true
  def handle_call(
        {:append, new_events},
        _from,
        %S{broadcaster: broadcaster, events: events} = state
      ) do
    Broadcaster.broadcast(broadcaster, {:new_events, new_events})
    {:reply, :ok, %{state | events: events ++ new_events}}
  end

  def handle_call({:subscribe, subscriber}, _from, %S{broadcaster: broadcaster} = state) do
    Broadcaster.subscribe(broadcaster, subscriber)
    {:reply, :ok, state}
  end

  def handle_call({:unsubscribe, subscriber}, _from, %S{broadcaster: broadcaster} = state) do
    Broadcaster.unsubscribe(broadcaster, subscriber)
    {:reply, :ok, state}
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

  def handle_call(:count, _from, %S{events: events} = state),
    do: {:reply, Enum.count(events), state}

  @impl true
  def handle_info(:timeout, state),
    do: {:stop, :normal, state}

  def handle_info({:EXIT, _, :normal}, state),
    do: {:stop, :shutdown, state}

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
