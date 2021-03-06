defmodule Derive.InMemoryReducerTest do
  use ExUnit.Case
  doctest Derive

  alias Derive.EventLog.InMemoryEventLog, as: EventLog

  defmodule User do
    defstruct [:id, :name, :email]
  end

  defmodule UserCreated do
    defstruct [:id, :user_id, :name, :email]
  end

  defmodule UserNameUpdated do
    defstruct [:id, :user_id, :name]
  end

  defmodule UserEmailUpdated do
    defstruct [:id, :user_id, :email]
  end

  defmodule UserDeactivated do
    defstruct [:id, :user_id]
  end

  defmodule UserReducer do
    use Derive.Reducer
    use Derive.ReducerState

    import Derive.State.InMemory.Operation

    alias Derive.MultiOp

    @impl true
    def partition(%{user_id: user_id}), do: user_id

    defp state, do: :users

    @impl true
    def handle_event(%UserCreated{user_id: user_id, name: name, email: email}) do
      merge({User, user_id}, %User{id: user_id, name: name, email: email})
    end

    def handle_event(%UserNameUpdated{user_id: user_id, name: name}) do
      merge({User, user_id}, %{name: name})
    end

    def handle_event(%UserEmailUpdated{user_id: user_id, email: email}) do
      merge({User, user_id}, %{email: email})
    end

    def handle_event(%UserDeactivated{user_id: user_id}) do
      delete({User, user_id})
    end

    @impl true
    def process_events(events, multi, %Derive.Options{logger: logger}) do
      Derive.Reducer.EventProcessor.process_events(
        events,
        multi,
        %Derive.Reducer.EventProcessor.Options{
          handle_event: &handle_event/1,
          get_cursor: &get_cursor/1,
          commit: &commit/1,
          on_error: :halt,
          logger: logger
        }
      )
    end

    @impl true
    def get_cursor(%{id: id}),
      do: id

    def commit(%MultiOp{} = op),
      do: Derive.State.InMemory.commit(state(), op)

    @impl true
    def reset_state,
      do: Derive.State.InMemory.reset_state(state())

    @impl true
    def needs_rebuild?,
      do: false

    @impl true
    def load_partition(_, id) do
      %Derive.Partition{
        id: id,
        cursor: 0,
        status: :ok
      }
    end

    @impl true
    def save_partition(_, _partition), do: :ok
  end

  test "processes events from an empty event log" do
    name = :in_memory_basic

    {:ok, event_log} = EventLog.start_link()

    {:ok, _sink} =
      Derive.State.InMemory.start_link(
        name: :users,
        reduce: &Derive.State.InMemory.Reduce.reduce/2
      )

    {:ok, _} = Derive.start_link(reducer: UserReducer, source: event_log, name: name)

    EventLog.append(event_log, [%UserCreated{id: "1", user_id: 99, name: "John"}])

    Derive.await(name, [
      %UserCreated{id: "1", user_id: 99, name: "John"}
    ])

    assert Derive.State.InMemory.get_state(:users) == %{
             User => %{
               99 => %User{id: 99, name: "John"}
             }
           }

    EventLog.append(event_log, [
      %UserNameUpdated{id: "2", user_id: 99, name: "Johny Darko"},
      %UserEmailUpdated{id: "3", user_id: 99, email: "john@hotmail.com"},
      %UserNameUpdated{id: "4", user_id: 99, name: "Donny Darko"}
    ])

    Derive.await(name, [
      %UserNameUpdated{id: "4", user_id: 99, name: "Donny Darko"}
    ])

    assert Derive.State.InMemory.get_state(:users) == %{
             User => %{
               99 => %User{id: 99, name: "Donny Darko", email: "john@hotmail.com"}
             }
           }

    Derive.stop(name)
  end
end
