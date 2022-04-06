defmodule DeriveEctoTest do
  use ExUnit.Case

  alias Derive.EventLog.InMemoryEventLog
  alias Derive.EventLog

  defmodule Stuff do
    defstruct [:id, :name]
  end

  test "it notifies subscribers when new events arrive" do
    {:ok, event_log} = InMemoryEventLog.start_link()
    EventLog.subscribe(event_log, self())

    EventLog.append(event_log, [:a])
    assert_received {:"$gen_cast", {:new_events, [:a]}}

    EventLog.append(event_log, [:b])
    assert_received {:"$gen_cast", {:new_events, [:b]}}
  end

  test "it allows fetching the first N records with a limit" do
    {:ok, event_log} = InMemoryEventLog.start_link()

    EventLog.append(event_log, [:a, :b, :c])

    assert {[:a, :b], cursor} = EventLog.fetch(event_log, {:start, 2})
    assert {[:c], cursor} = EventLog.fetch(event_log, {cursor, 2})
    assert {[], cursor} = EventLog.fetch(event_log, {cursor, 2})
  end

  test "streaming events" do
    {:ok, event_log} = InMemoryEventLog.start_link()

    EventLog.append(event_log, [:a, :b, :c, :d, :e, :f])

    events = EventLog.stream(event_log, batch_size: 2) |> Enum.to_list()

    assert events == [:a, :b, :c, :d, :e, :f]
  end
end
