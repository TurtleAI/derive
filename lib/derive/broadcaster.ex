defmodule Derive.Broadcaster do
  @moduledoc """
  A utility module for broadcasting a message to subscribers
  """

  use GenServer

  @type t :: %__MODULE__{
          subscribers: [subscriber()]
        }
  defstruct subscribers: []

  @type server :: pid()
  @type subscriber :: pid()

  alias __MODULE__, as: S

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, %S{}, opts)

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, opts}
    }
  end

  ### Client

  @doc """
  Append a list of events to the event log
  """
  def broadcast(server, message),
    do: GenServer.cast(server, {:broadcast, message})

  @doc """
  Subscribe to the event log so that when new events appear in it,
  the subscriber will be notified.

  Currently, the only notification sent to the subscriber is of the format {:new_events, events}
  """
  @spec subscribe(server(), subscriber()) :: :ok
  def subscribe(server, subscriber),
    do: GenServer.call(server, {:subscribe, subscriber})

  @doc """
  Remove a subscriber that was previously subscribed
  """
  @spec unsubscribe(server(), subscriber()) :: :ok
  def unsubscribe(server, subscriber),
    do: GenServer.call(server, {:unsubscribe, subscriber})

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

  @impl true
  def handle_cast(
        {:broadcast, message},
        %S{subscribers: subscribers} = state
      ) do
    broadcast_to_subscribers(subscribers, message)
    {:noreply, state}
  end

  @impl true
  def handle_info(:timeout, state),
    do: {:stop, :normal, state}

  def handle_info({:EXIT, _, :normal}, state),
    do: {:stop, :shutdown, state}

  def handle_info({:DOWN, _ref, :process, pid, _}, state),
    do: {:noreply, remove_subscriber(state, pid)}

  defp broadcast_to_subscribers([], _events),
    do: :ok

  defp broadcast_to_subscribers([subscriber | rest], message) do
    GenServer.cast(subscriber, message)
    broadcast_to_subscribers(rest, message)
  end

  defp add_subscriber(%S{subscribers: subscribers} = state, subscriber) do
    %{state | subscribers: subscribers ++ [subscriber]}
  end

  defp remove_subscriber(%S{subscribers: subscribers} = state, subscriber) do
    %{state | subscribers: Enum.reject(subscribers, &(&1 == subscriber))}
  end
end
