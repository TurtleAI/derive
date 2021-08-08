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

    def source, do: :events
    def partition(%{user_id: user_id}), do: user_id
    def sink, do: :users

    def handle(%UserCreated{user_id: user_id, name: name, email: email}) do
      merge([User, user_id], %User{id: user_id, name: name, email: email})
    end

    def handle(%UserNameUpdated{user_id: user_id, name: name}) do
      merge([User, user_id], %{name: name})
    end

    def handle(%UserEmailUpdated{user_id: user_id, email: email}) do
      merge([User, user_id], %{email: email})
    end

    def handle(%UserDeactivated{user_id: user_id}) do
      delete([User, user_id])
    end

    def handle_error(%Derive.Reducer.Error{} = error) do
      {:retry, Derive.Reducer.Error.events_without_failed_events(error)}
    end
  end

  test "processes events" do
    {:ok, _event_log} = Derive.Source.EventLog.start_link(name: :events)

    {:ok, _sink} =
      Derive.Sink.InMemory.start_link(name: :users, reduce: &Derive.Sink.InMemory.Reduce.reduce/2)

    {:ok, dispatcher} = Derive.Dispatcher.start_link(UserReducer, mode: :catchup)

    Derive.Source.EventLog.append(:events, [%UserCreated{id: 1, user_id: 99, name: "John"}])

    Derive.Dispatcher.wait_for_catchup(dispatcher)

    assert Derive.Sink.InMemory.fetch(:users) == %{
             User => %{
               99 => %User{id: 99, name: "John"}
             }
           }

    Derive.Source.EventLog.append(:events, [
      %UserNameUpdated{id: 2, user_id: 99, name: "Johny Darko"},
      %UserEmailUpdated{user_id: 99, email: "john@hotmail.com"}
    ])

    Derive.Dispatcher.wait_for_catchup(dispatcher)

    assert Derive.Sink.InMemory.fetch(:users) == %{
             User => %{
               99 => %User{id: 99, name: "Johny Darko", email: "john@hotmail.com"}
             }
           }

    # completes immediately since we are already caught up
    Derive.Dispatcher.wait_for_catchup(dispatcher)

    assert Derive.Sink.InMemory.fetch(:users) == %{
             User => %{
               99 => %User{id: 99, name: "Johny Darko", email: "john@hotmail.com"}
             }
           }
  end
end
