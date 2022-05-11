defmodule Derive.Reducer.EventProcessorTest do
  use ExUnit.Case

  alias Derive.Partition
  alias Derive.Reducer.EventProcessor
  alias Derive.State.MultiOp

  def create_state(initial) do
    {:ok, pid} = Agent.start_link(fn -> initial end)
    pid
  end

  def update_state(pid, func), do: Agent.update(pid, func)
  def get_state(pid), do: Agent.get(pid, fn x -> x end)

  test "processes events successfully" do
    state = create_state([])

    handle_event = fn %{name: name} ->
      name
    end

    get_cursor = fn %{id: id} -> id end

    commit = fn multi ->
      update_state(state, fn list ->
        list ++ MultiOp.operations(multi)
      end)

      MultiOp.committed(multi)
    end

    events = [
      %{id: "1", name: "bob"},
      %{id: "2", name: "jones"}
    ]

    multi =
      EventProcessor.process_events(
        events,
        MultiOp.new(%Partition{id: "x", cursor: :start, status: :ok}),
        {handle_event, get_cursor, commit},
        []
      )

    assert %{id: "x", cursor: "2", status: :ok} = multi.partition

    assert get_state(state) == ["bob", "jones"]
  end
end
