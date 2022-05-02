defmodule Derive.EventLog.QueryEventLog do
  @moduledoc """
  An event log backed by any queryable source.
  For example, one backed by an Ecto table or a Mongo DB.
  The a particular implementation only needs to implement a fetch and count.
  """

  use GenServer

  alias Derive.EventLog

  @type t :: %__MODULE__{
          subscribers: [pid()]
        }
  defstruct [:fetch, :count, subscribers: []]

  @type option ::
          {:repo, Ecto.Repo.t()}
          | {:table, binary()}
          | {:event_types, [atom()]}
          | {:fetch, fetch()}
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
  def notify(server, events),
    do: GenServer.cast(server, {:new_events, events})

  ### Server

  @impl true
  def init(state),
    do: {:ok, state}

  @impl true
  def handle_call({:subscribe, subscriber}, {pid, _}, state) do
    # If this subscriber process terminates, we want to monitor it and remove it
    # from the list of subscribers so we aren't sending messages to dead processes
    Process.monitor(pid)
    {:reply, :ok, add_subscriber(state, subscriber)}
  end

  def handle_call({:unsubscribe, subscriber}, _from, state) do
    {:reply, :ok, remove_subscriber(state, subscriber)}
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
  def handle_cast({:new_events, events}, %S{subscribers: subscribers} = state) do
    notify_subscribers(subscribers, {:new_events, events})
    {:noreply, state}
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
end
