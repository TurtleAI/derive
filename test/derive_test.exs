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
      Process.sleep(1_000)
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
  end

  test "processes events from an empty event log" do
    {:ok, _event_log} = Derive.Source.EventLog.start_link(name: :events)

    {:ok, _sink} =
      Derive.Sink.InMemory.start_link(name: :users, reduce: &Derive.Sink.InMemory.Reduce.reduce/2)

    {:ok, dispatcher} = Derive.Dispatcher.start_link(UserReducer, mode: :catchup)

    IO.puts("appendy")
    Derive.Source.EventLog.append(:events, [%UserCreated{id: 1, user_id: 99, name: "John"}])

    IO.inspect({DateTime.utc_now(), "Derive.Dispatcher.await_processed"})

    Derive.Dispatcher.await_processed(dispatcher, [
      %UserCreated{id: 12, user_id: 99, name: "John"}
    ])

    IO.puts("await_processed_done")

    assert Derive.Sink.InMemory.fetch(:users) == %{
             User => %{
               99 => %User{id: 99, name: "John"}
             }
           }

    # Derive.Source.EventLog.append(:events, [
    #   %UserNameUpdated{id: 2, user_id: 99, name: "Johny Darko"},
    #   %UserEmailUpdated{id: 3, user_id: 99, email: "john@hotmail.com"},
    #   %UserNameUpdated{id: 4, user_id: 99, name: "Donny Darko"}
    # ])

    # Derive.Dispatcher.await_processed(dispatcher, [
    #   %UserNameUpdated{id: 4, user_id: 99, name: "Donny Darko"}
    # ])

    # assert Derive.Sink.InMemory.fetch(:users) == %{
    #          User => %{
    #            99 => %User{id: 99, name: "Donny Darko", email: "john@hotmail.com"}
    #          }
    #        }
  end
end
