defmodule DeriveEctoTest do
  use ExUnit.Case

  defmodule User do
    use Derive.State.Ecto.Model

    @primary_key {:id, :string, []}
    schema "users" do
      field(:name, :string)
      field(:email, :string)
    end

    def up do
      create table(:users, primary_key: false) do
        add(:id, :string, size: 32, primary_key: true)
        add(:name, :string, size: 512)
        add(:email, :string, size: 256)
      end
    end

    def down, do: drop_if_exists(table(:users))
  end

  defmodule UserCreated do
    defstruct [:id, :user_id, :name, :email]
  end

  defmodule UserNameUpdated do
    defstruct [:id, :user_id, :name]
  end

  defmodule UserReducer do
    use Derive.Reducer

    import Derive.State.Ecto.Operation

    def source, do: :events
    def partition(%{user_id: user_id}), do: user_id

    def handle_event(%UserCreated{user_id: user_id, name: name, email: email}) do
      insert(%User{id: user_id, name: name, email: email})
    end

    def handle_event(%UserNameUpdated{user_id: user_id, name: name}) do
      update([User, user_id], %{name: name})
    end

    def commit_operations(operations) do
      Derive.State.Ecto.commit(Derive.Repo, operations)
    end
  end

  setup_all do
    Derive.State.Ecto.reset_state(Derive.Repo, [User])

    :ok
  end

  setup do
    # Explicitly get a connection before each test
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Derive.Repo)
    # Setting the shared mode must be done only after checkout
    Ecto.Adapters.SQL.Sandbox.mode(Derive.Repo, {:shared, self()})
  end

  test "insert a user" do
    {:ok, _event_log} = Derive.Source.EventLog.start_link(name: :events)
    {:ok, dispatcher} = Derive.Dispatcher.start_link(UserReducer)

    Derive.Source.EventLog.append(:events, [%UserCreated{id: "1", user_id: "99", name: "John"}])

    Derive.Dispatcher.await(dispatcher, [
      %UserCreated{id: "1", user_id: "99", name: "John"}
    ])

    user = Derive.Repo.get(User, "99")
    assert user.name == "John"

    Derive.Source.EventLog.append(:events, [
      %UserNameUpdated{id: "2", user_id: "99", name: "John Wayne"}
    ])

    Derive.Dispatcher.await(dispatcher, [
      %UserNameUpdated{id: "2", user_id: "99", name: "John Wayne"}
    ])

    user = Derive.Repo.get(User, "99")
    assert user.name == "John Wayne"
  end
end
