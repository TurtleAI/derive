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

  def handle_event(%{name: name}), do: name

  def get_cursor(%{id: id}), do: id

  defmodule TestCommitError do
    defexception [:message]
  end

  def commit(state, multi) do
    update_state(state, fn list ->
      list ++ MultiOp.operations(multi)
    end)

    MultiOp.committed(multi)
  end

  test "processes events" do
    state = create_state([])

    multi =
      EventProcessor.process_events(
        [
          %{id: "1", name: "bob"},
          %{id: "2", name: "jones"}
        ],
        MultiOp.new(%Partition{id: "x", cursor: :start, status: :ok}),
        %EventProcessor{
          handle_event: &handle_event/1,
          get_cursor: &get_cursor/1,
          commit: &commit(state, &1)
        }
      )

    assert %{id: "x", cursor: "2", status: :ok} = multi.partition

    assert get_state(state) == ["bob", "jones"]
  end

  test "skips already processed events" do
    state = create_state([])

    multi =
      EventProcessor.process_events(
        [
          %{id: "5", name: "bob"},
          %{id: "6", name: "jones"},
          %{id: "7", name: "bruce lee"}
        ],
        MultiOp.new(%Partition{id: "x", cursor: "6", status: :ok}),
        %EventProcessor{
          handle_event: &handle_event/1,
          get_cursor: &get_cursor/1,
          commit: &commit(state, &1)
        }
      )

    assert %MultiOp{status: :committed, operations: operations} = multi

    # operations are reversed
    assert [
             %Derive.State.EventOp{
               cursor: "7",
               event: %{id: "7", name: "bruce lee"},
               operations: ["bruce lee"],
               status: :ok
             },
             %Derive.State.EventOp{
               cursor: "6",
               event: %{id: "6", name: "jones"},
               operations: [],
               status: :skip
             },
             %Derive.State.EventOp{
               cursor: "5",
               event: %{id: "5", name: "bob"},
               operations: [],
               status: :skip
             }
           ] = operations

    assert %{id: "x", cursor: "7", status: :ok} = multi.partition

    assert get_state(state) == ["bruce lee"]
  end

  test "stop processing events if there is a handle_event/1 error" do
    state = create_state([])

    multi =
      EventProcessor.process_events(
        [
          %{id: "1", name: "a"},
          %{id: "2", name: "b"},
          # pattern match will cause handle_event to fail
          %{id: "3", error: "blah"},
          %{id: "4", name: "c"}
        ],
        MultiOp.new(%Partition{id: "x", cursor: :start, status: :ok}),
        %EventProcessor{
          handle_event: &handle_event/1,
          get_cursor: &get_cursor/1,
          commit: &commit(state, &1)
        }
      )

    assert %MultiOp{status: :error, error: {:handle_event, event_op}} = multi

    assert %Derive.State.EventOp{
             cursor: "3",
             error: %FunctionClauseError{},
             event: %{error: "blah", id: "3"},
             operations: [],
             status: :error
           } = event_op

    assert %{id: "x", cursor: "4", status: :error} = multi.partition

    # didn't even get to committing the events
    assert get_state(state) == []
  end

  test "stop processing events if there is a commit error" do
    error_commit = fn state, multi ->
      ops = MultiOp.operations(multi)

      cond do
        Enum.member?(ops, "exception") ->
          raise %TestCommitError{message: "exception!"}

        Enum.member?(ops, "error") ->
          MultiOp.commit_failed(multi, %TestCommitError{message: "error!"})

        true ->
          commit(state, ops)
      end
    end

    # when an exception is thrown
    state = create_state([])

    multi =
      EventProcessor.process_events(
        [
          %{id: "1", name: "a"},
          %{id: "2", name: "exception"}
        ],
        MultiOp.new(%Partition{id: "x", cursor: :start, status: :ok}),
        %EventProcessor{
          handle_event: &handle_event/1,
          get_cursor: &get_cursor/1,
          commit: fn multi -> error_commit.(state, multi) end
        }
      )

    assert %MultiOp{status: :error, error: {:commit, error}} = multi
    assert %TestCommitError{message: "exception!"} = error

    # when a commit gracefully fails
    state = create_state([])

    multi =
      EventProcessor.process_events(
        [
          %{id: "1", name: "a"},
          %{id: "2", name: "error"}
        ],
        MultiOp.new(%Partition{id: "x", cursor: :start, status: :ok}),
        %EventProcessor{
          handle_event: &handle_event/1,
          get_cursor: &get_cursor/1,
          commit: fn multi -> error_commit.(state, multi) end
        }
      )

    assert %MultiOp{status: :error, error: {:commit, error}} = multi
    assert %TestCommitError{message: "error!"} = error
  end
end
