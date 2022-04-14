defmodule DeriveInMemoryEventLogTest do
  use ExUnit.Case

  alias Derive.EventLog.InMemoryEventLog
  alias Derive.EventLog

  defmodule Stuff do
    defstruct [:id, :name]
  end

  defmodule Event do
    defstruct [:id]
  end

  def events(atoms) do
    Enum.map(atoms, fn id -> %Event{id: id} end)
  end

  test "it notifies subscribers when new events arrive" do
    {:ok, event_log} = InMemoryEventLog.start_link()
    EventLog.subscribe(event_log, self())

    InMemoryEventLog.append(event_log, ["a"])
    assert_received {:"$gen_cast", {:new_events, ["a"]}}

    InMemoryEventLog.append(event_log, ["b"])
    assert_received {:"$gen_cast", {:new_events, ["b"]}}
  end

  test "it allows fetching the first N records with a limit" do
    {:ok, event_log} = InMemoryEventLog.start_link()

    InMemoryEventLog.append(event_log, events(["a", "b", "c"]))

    assert {[%Event{id: "a"}, %Event{id: "b"}], cursor} = EventLog.fetch(event_log, {:start, 2})
    assert {[%Event{id: "c"}], cursor} = EventLog.fetch(event_log, {cursor, 2})
    assert {[], _cursor} = EventLog.fetch(event_log, {cursor, 2})
  end

  test "streaming events" do
    {:ok, event_log} = InMemoryEventLog.start_link()

    InMemoryEventLog.append(event_log, events(["a", "b", "c", "d", "e", "f"]))

    events = EventLog.stream(event_log, batch_size: 2) |> Enum.to_list()

    assert events == events(["a", "b", "c", "d", "e", "f"])
  end

  test "streaming events in the middle" do
    {:ok, event_log} = InMemoryEventLog.start_link()

    InMemoryEventLog.append(event_log, events(["a", "b", "c", "d", "e", "f"]))

    events = EventLog.stream(event_log, cursor: "3", batch_size: 2) |> Enum.to_list()

    assert events == events(["d", "e", "f"])
  end
end
