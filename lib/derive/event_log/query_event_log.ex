defmodule Derive.EventLog.QueryEventLog do
  @moduledoc """
  An event log backed by any queryable source.
  For example, one backed by an Ecto table or a Mongo DB.
  The a particular implementation only needs to implement a fetch and count.
  """

  use GenServer

  alias Derive.EventLog
  alias Derive.Broadcaster

  @type t :: %__MODULE__{
          broadcaster: Broadcaster.t()
        }
  defstruct [:fetch, :count, :broadcaster]

  @type option ::
          {:fetch, fetch()}
          | {:count, count()}
          | GenServer.option()

  @typedoc """
  A function, given a {cursor, limit}, to fetch a slice of events
  """
  @type fetch :: (EventLog.slice() -> list(EventLog.event()))

  @typedoc """
  A function to return the total number of events at the moment
  """
  @type count :: (() -> non_neg_integer())

  alias __MODULE__, as: S

  @spec start_link([option]) :: {:ok, pid()}
  def start_link(opts \\ []) do
    {opts, genserver_opts} = Keyword.split(opts, [:fetch, :count])

    fetch = Keyword.fetch!(opts, :fetch)
    count = Keyword.fetch!(opts, :count)

    GenServer.start_link(
      __MODULE__,
      %S{fetch: fetch, count: count},
      genserver_opts
    )
  end

  ### Client

  @doc """
  When new events have been inserted into the database, notify all subscribers
  """
  def broadcast(server, events),
    do: GenServer.call(server, {:broadcast, events})

  ### Server

  @impl true
  def init(state) do
    {:ok, broadcaster} = Broadcaster.start_link()
    {:ok, %{state | broadcaster: broadcaster}}
  end

  @impl true
  def handle_call({:subscribe, subscriber}, _from, %S{broadcaster: broadcaster} = state) do
    Broadcaster.subscribe(broadcaster, subscriber)
    {:reply, :ok, state}
  end

  def handle_call({:unsubscribe, subscriber}, _from, %S{broadcaster: broadcaster} = state) do
    Broadcaster.unsubscribe(broadcaster, subscriber)
    {:reply, :ok, state}
  end

  def handle_call({:broadcast, events}, _from, %S{broadcaster: broadcaster} = state) do
    Broadcaster.broadcast(broadcaster, {:new_events, events})
    {:reply, :ok, state}
  end

  def handle_call(
        {:fetch, {cursor, limit}},
        _from,
        %S{fetch: fetch} = state
      ) do
    case fetch.({cursor, limit}) do
      [] ->
        # we're at the end, so we keep the same cursor
        {:reply, {[], cursor}, state}

      events ->
        # we're ordering by id, so can take the id of the last event
        %{id: new_cursor} = Enum.at(events, -1)
        {:reply, {events, new_cursor}, state}
    end
  end

  def handle_call(:count, _from, %S{count: count} = state),
    do: {:reply, count.(), state}

  @impl true
  def handle_info(:timeout, state),
    do: {:stop, :normal, state}

  def handle_info({:EXIT, _, :normal}, state),
    do: {:stop, :shutdown, state}
end
