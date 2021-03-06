defmodule Derive.EventLog do
  @moduledoc """
  The interface for a persistent, ordered log of events.
  They can be stored anywhere such as in-memory, in a postgres table, or on disk.

  A `Derive.Dispatcher` will subscribe to an event log and keep some state up to date
  based on the events in the log.

  In the production app, we will be using a events table backed EventLog

  Has an in-memory implementation `Derive.EventLog.InMemoryEventLog` for testing purposes.
  """

  @typedoc """
  A generic struct that represents an event.
  """
  @type event() :: any()

  @typedoc """
  An opaque value that indicates the position of an event in an event log

  A cursor must have the following properties:
  - It must be unique to the entire event log
  - It must be in increasing order
  - It must be stable, meaning a cursor should not change per event
  """
  @type cursor() :: any()

  @type option :: {:cursor, cursor()} | {:batch_size, non_neg_integer()}

  @typedoc """
  Used for pagination.
  A cursor and a limit on how many records to fetch.
  """
  @type slice :: {cursor(), non_neg_integer()}

  @doc """
  Subscribe to the event log so that when new events appear in it,
  the subscriber will be notified.

  Currently, the only notification sent to the subscriber is of the format {:new_events, events}
  """
  @spec subscribe(pid(), pid()) :: :ok
  def subscribe(server, subscriber),
    do: GenServer.call(server, {:subscribe, subscriber})

  @doc """
  Remove a subscriber that was previously subscribed
  """
  @spec unsubscribe(pid(), pid()) :: :ok
  def unsubscribe(server, subscriber),
    do: GenServer.call(server, {:unsubscribe, subscriber})

  @doc """
  Fetch a list of events that have been persisted to the event log
  The events can be paginated through {cursor, limit}

  cursor: fetch all events after this currsor
  limit: fetch a maximum of this many events

  The return value is {events, cursor}
  events: the events fetched
  cursor: a pointer to the last fetched event which can be used for paginating the event log
    calling fetch again with this cursor will fetch the next batch of events after the cursor
    to start at the beginning, can use the special keyword :start

  If there are no more events, an empty list will be returned
  """
  @spec fetch(pid(), slice()) :: {[event()], cursor()}
  def fetch(server, {cursor, limit}, timeout \\ 30_000),
    do: GenServer.call(server, {:fetch, {cursor, limit}}, timeout)

  @doc """
  Return a stream of all the events in this event log,
  Starts from the very first event to the very last event.

  Options:
  batch_size: The max chunk size with which to call fetch. Defaults to 100

  Interally makes use of `Derive.EventLog.fetch/2` to lazily fetch the events in batches
  """
  @spec stream(pid(), [option()]) :: Enum.t()
  def stream(server, opts \\ []) do
    cursor = Keyword.get(opts, :cursor, :start)
    batch_size = Keyword.get(opts, :batch_size, 100)

    Stream.resource(
      fn -> cursor end,
      fn cursor ->
        case fetch(server, {cursor, batch_size}) do
          {[], _cursor} -> {:halt, nil}
          {events, new_cursor} -> {events, new_cursor}
        end
      end,
      fn _ -> :ok end
    )
  end

  @doc """
  Return the total number of events in the event log
  """
  @spec count(pid()) :: non_neg_integer()
  def count(server, timeout \\ 120_000),
    do: GenServer.call(server, :count, timeout)
end
