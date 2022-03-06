defmodule DeriveTest do
  use ExUnit.Case
  doctest Derive

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

    def source, do: :events
    def partition(%{user_id: user_id}), do: user_id

    def handle_event(%UserCreated{user_id: user_id, name: name, email: email}) do
      merge([User, user_id], %User{id: user_id, name: name, email: email})
    end

    def handle_event(%UserNameUpdated{user_id: user_id, name: name}) do
      Process.sleep(500)
      merge([User, user_id], %{name: name})
    end

    def handle_event(%UserEmailUpdated{user_id: user_id, email: email}) do
      merge([User, user_id], %{email: email})
    end

    def handle_event(%UserDeactivated{user_id: user_id}) do
      delete([User, user_id])
    end

    def commit_operations(operations) do
      Derive.State.InMemory.commit(:users, operations)
    end
  end

  test "processes events from an empty event log" do
    {:ok, _event_log} = Derive.Source.EventLog.start_link(name: :events)

    {:ok, _sink} =
      Derive.State.InMemory.start_link(
        name: :users,
        reduce: &Derive.State.InMemory.Reduce.reduce/2
      )

    {:ok, dispatcher} = Derive.Dispatcher.start_link(UserReducer, mode: :catchup)

    Derive.Source.EventLog.append(:events, [%UserCreated{id: 1, user_id: 99, name: "John"}])

    Derive.Dispatcher.await_processed(dispatcher, [
      %UserCreated{id: 12, user_id: 99, name: "John"}
    ])

    assert Derive.State.InMemory.get_state(:users) == %{
             User => %{
               99 => %User{id: 99, name: "John"}
             }
           }

    Derive.Source.EventLog.append(:events, [
      %UserNameUpdated{id: 2, user_id: 99, name: "Johny Darko"},
      %UserEmailUpdated{id: 3, user_id: 99, email: "john@hotmail.com"},
      %UserNameUpdated{id: 4, user_id: 99, name: "Donny Darko"}
    ])

    Derive.Dispatcher.await_processed(dispatcher, [
      %UserNameUpdated{id: 4, user_id: 99, name: "Donny Darko"}
    ])

    assert Derive.State.InMemory.get_state(:users) == %{
             User => %{
               99 => %User{id: 99, name: "Donny Darko", email: "john@hotmail.com"}
             }
           }
  end
end
