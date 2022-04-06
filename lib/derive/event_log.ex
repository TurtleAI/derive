defmodule Derive.EventLog do
  @moduledoc """
  The interface for a persistent, ordered log of events.
  They can be stored anywhere such as in-memory, in a postgres table, or on disk.

  A `Derive.Dispatcher` will subscribe to an event log and keep some state up to date
  based on the events in the log.

  In the production app, we will be using a events table backed EventLog

  Has an in-memory implementation `Derive.EventLog.InMemoryEventLog` for testing purposes.
  """

  @doc """
  Subscribe to the event log so that when new events appear in it,
  the subscriber will be notified.

  Currently, the only notification sent to the subscriber is of the format {:new_events, events}
  """
  def subscribe(server, subscriber) do
    GenServer.call(server, {:subscribe, subscriber})
  end

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
  def fetch(server, {cursor, limit}) do
    GenServer.call(server, {:fetch, {cursor, limit}})
  end

  @doc """
  Return a stream of all the events in this event log,
  Starts from the very first event to the very last event.

  Options:
  batch_size: The max chunk size with which to call fetch. Defaults to 100

  Interally makes use of `Derive.EventLog.fetch/2` to lazily fetch the events in batches
  """
  def stream(server, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 100)

    Stream.resource(
      fn -> :start end,
      fn cursor ->
        case fetch(server, {cursor, batch_size}) do
          {[], _cursor} -> {:halt, nil}
          {events, new_cursor} -> {events, new_cursor}
        end
      end,
      fn nil -> :ok end
    )
  end
end
