defmodule DeriveInMemoryTest do
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

    import Derive.State.InMemory.Operation

    alias Derive.State.MultiOp

    def partition(%{user_id: user_id}), do: user_id

    defp state, do: :users

    def handle_event(%UserCreated{user_id: user_id, name: name, email: email}) do
      merge([User, user_id], %User{id: user_id, name: name, email: email})
    end

    def handle_event(%UserNameUpdated{user_id: user_id, name: name}) do
      merge([User, user_id], %{name: name})
    end

    def handle_event(%UserEmailUpdated{user_id: user_id, email: email}) do
      merge([User, user_id], %{email: email})
    end

    def handle_event(%UserDeactivated{user_id: user_id}) do
      delete([User, user_id])
    end

    def reduce_events(events, partition) do
      Derive.Reducer.EventProcessor.reduce_events(
        events,
        MultiOp.new(partition),
        &handle_event/1,
        on_error: :halt
      )
    end

    def processed_event?(%{cursor: cursor}, %{id: id}),
      do: cursor >= id

    def commit(%MultiOp{} = op),
      do: Derive.State.InMemory.commit(state(), op)

    def reset_state,
      do: Derive.State.InMemory.reset_state(state())

    def get_partition(id) do
      %Derive.Partition{
        id: id,
        cursor: 0,
        status: :ok
      }
    end

    def set_partition(_partition), do: :ok
  end

  test "processes events from an empty event log" do
    {:ok, event_log} = EventLog.start_link()

    {:ok, _sink} =
      Derive.State.InMemory.start_link(
        name: :users,
        reduce: &Derive.State.InMemory.Reduce.reduce/2
      )

    {:ok, _} = Derive.start_link(reducer: UserReducer, source: event_log, name: :t1)

    EventLog.append(event_log, [%UserCreated{id: "1", user_id: 99, name: "John"}])

    Derive.await(:t1, [
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

    Derive.await(:t1, [
      %UserNameUpdated{id: "4", user_id: 99, name: "Donny Darko"}
    ])

    assert Derive.State.InMemory.get_state(:users) == %{
             User => %{
               99 => %User{id: 99, name: "Donny Darko", email: "john@hotmail.com"}
             }
           }
  end
end
