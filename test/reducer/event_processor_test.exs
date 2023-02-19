defmodule Derive.Reducer.EventProcessorTest do
  use ExUnit.Case

  alias Derive.{Partition, MultiOp}
  alias Derive.Reducer.EventProcessor
  alias Derive.Reducer.EventProcessor.Options
  alias Derive.Error.{HandleEventError, CommitError}

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
        %Options{
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
        %Options{
          handle_event: &handle_event/1,
          get_cursor: &get_cursor/1,
          commit: &commit(state, &1)
        }
      )

    assert %MultiOp{status: :committed, operations: operations} = multi

    # operations are reversed
    assert [
             %Derive.EventOp{
               cursor: "7",
               event: %{id: "7", name: "bruce lee"},
               operations: ["bruce lee"],
               status: :ok
             },
             %Derive.EventOp{
               cursor: "6",
               event: %{id: "6", name: "jones"},
               operations: [],
               status: :ignore
             },
             %Derive.EventOp{
               cursor: "5",
               event: %{id: "5", name: "bob"},
               operations: [],
               status: :ignore
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
        %Options{
          handle_event: &handle_event/1,
          get_cursor: &get_cursor/1,
          commit: &commit(state, &1)
        }
      )

    assert %MultiOp{status: :error, error: %HandleEventError{operation: event_op}} = multi

    assert %Derive.EventOp{
             cursor: "3",
             error: {%FunctionClauseError{}, [_ | _]},
             event: %{error: "blah", id: "3"},
             operations: [],
             status: :error
           } = event_op

    assert %{id: "x", cursor: "2", status: :error} = multi.partition

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
          MultiOp.failed(multi, %CommitError{
            commit: :error_commit,
            error: %TestCommitError{message: "error!"},
            operations: MultiOp.event_operations(multi)
          })

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
        %Options{
          handle_event: &handle_event/1,
          get_cursor: &get_cursor/1,
          commit: fn multi -> error_commit.(state, multi) end
        }
      )

    assert %MultiOp{status: :error, error: %CommitError{error: error}} = multi
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
        %Options{
          handle_event: &handle_event/1,
          get_cursor: &get_cursor/1,
          commit: fn multi -> error_commit.(state, multi) end
        }
      )

    assert %MultiOp{status: :error, error: %CommitError{error: error}} = multi
    assert %TestCommitError{message: "error!"} = error
  end
end
