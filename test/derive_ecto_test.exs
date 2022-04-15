defmodule DeriveEctoTest do
  use ExUnit.Case

  alias Derive.EventLog.InMemoryEventLog

  @same_time_threshold 10

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

  defmodule UserReducerPartitions do
    use Derive.State.Ecto.Model

    @primary_key {:id, :string, []}
    schema "user_reducer_partitions" do
      field(:version, :string)
    end

    def up do
      create table(:user_reducer_partitions, primary_key: false) do
        add(:id, :string, size: 32, primary_key: true)
        add(:version, :string, size: 32)
      end
    end

    def down, do: drop_if_exists(table(:user_reducer_partitions))
  end

  defmodule LogEntry do
    use Derive.State.Ecto.Model

    schema "log_entries" do
      field(:message, :string)
      field(:timestamp, :integer)
    end

    def up do
      create table(:log_entries) do
        add(:message, :string, size: 512)
        add(:timestamp, :bigint)
      end
    end

    def down, do: drop_if_exists(table(:log_entries))
  end

  defmodule UserCreated do
    defstruct [:id, :user_id, :name, :email, sleep: 0]
  end

  defmodule UserNameUpdated do
    defstruct [:id, :user_id, :name, sleep: 0]
  end

  defmodule UserReducer do
    use Derive.Reducer

    import Derive.State.Ecto.Operation

    alias Derive.State.MultiOp

    def source, do: :events
    def partition(%{user_id: user_id}), do: user_id

    defp maybe_sleep(0), do: :ok
    defp maybe_sleep(timeout), do: Process.sleep(timeout)

    defp log(message) do
      insert(%LogEntry{message: message, timestamp: :os.system_time(:millisecond)})
    end

    def handle_event(%UserCreated{user_id: user_id, name: name, email: email, sleep: sleep}) do
      maybe_sleep(sleep)

      [
        log("created-#{user_id}"),
        insert(%User{
          id: user_id,
          name: name,
          email: email
        })
      ]
    end

    def handle_event(%UserNameUpdated{user_id: user_id, name: name, sleep: sleep}) do
      maybe_sleep(sleep)

      [
        log("updated-#{user_id}"),
        update([User, user_id], %{name: name})
      ]
    end

    def commit_operations(%MultiOp{} = op) do
      operations =
        MultiOp.operations(op) ++
          [
            %Derive.State.Ecto.Operation.SetPartitionVersion{
              table: UserReducerPartitions,
              partition: op.partition,
              version: MultiOp.partition_version(op)
            }
          ]

      Derive.State.Ecto.commit(Derive.Repo, operations)
    end

    def get_version(partition) do
      case Derive.Repo.get(UserReducerPartitions, partition) do
        nil -> :start
        %{version: version} -> version
      end
    end

    def set_version(partition, version) do
      Derive.State.Ecto.commit(Derive.Repo, [
        %Derive.State.Ecto.Operation.SetPartitionVersion{
          table: UserReducerPartitions,
          partition: partition,
          version: version
        }
      ])
    end

    def reset_state do
      Derive.State.Ecto.reset_state(Derive.Repo, [UserReducerPartitions, User, LogEntry])
    end
  end

  setup_all do
    UserReducer.reset_state()

    :ok
  end

  def get_logs() do
    for %{message: message, timestamp: timestamp} <- Derive.Repo.all(LogEntry) do
      {message, timestamp}
    end
    |> Enum.sort_by(fn {message, timestamp} ->
      {timestamp, message}
    end)
  end

  setup do
    # Explicitly get a connection before each test
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Derive.Repo)
    # Setting the shared mode must be done only after checkout
    Ecto.Adapters.SQL.Sandbox.mode(Derive.Repo, {:shared, self()})
  end

  test "insert a user" do
    {:ok, _event_log} = InMemoryEventLog.start_link(name: :events)
    {:ok, dispatcher} = Derive.Dispatcher.start_link(UserReducer)

    InMemoryEventLog.append(:events, [
      %UserCreated{id: "1", user_id: "99", name: "John"}
    ])

    Derive.Dispatcher.await(dispatcher, [
      %UserCreated{id: "1", user_id: "99", name: "John"}
    ])

    user = Derive.Repo.get(User, "99")
    assert user.name == "John"

    InMemoryEventLog.append(:events, [
      %UserNameUpdated{id: "2", user_id: "99", name: "John Wayne"}
    ])

    Derive.Dispatcher.await(dispatcher, [
      %UserNameUpdated{id: "2", user_id: "99", name: "John Wayne"}
    ])

    user = Derive.Repo.get(User, "99")
    assert user.name == "John Wayne"
  end

  test "events are processed in parallel according to the partition" do
    {:ok, _event_log} = InMemoryEventLog.start_link(name: :events)
    {:ok, dispatcher} = Derive.Dispatcher.start_link(UserReducer)

    events = [
      %UserCreated{id: "1", user_id: "s", name: "Same", sleep: 100},
      %UserNameUpdated{id: "2", user_id: "s", name: "Similar", sleep: 100},
      %UserCreated{id: "3", user_id: "t", name: "Time", sleep: 100}
    ]

    InMemoryEventLog.append(:events, events)
    Derive.Dispatcher.await(dispatcher, events)

    assert [{"created-s", t1}, {"created-t", t2}, {"updated-s", t3}] = get_logs()

    assert t2 - t1 <= @same_time_threshold

    assert t3 - t2 > @same_time_threshold

    [same, time] = [Derive.Repo.get(User, "s"), Derive.Repo.get(User, "t")]

    assert same.name == "Similar"
    assert time.name == "Time"
  end

  test "resuming a dispatcher after a server is restarted" do
    {:ok, _event_log} = InMemoryEventLog.start_link(name: :events)
    {:ok, dispatcher} = Derive.Dispatcher.start_link(UserReducer)

    events = [
      %UserCreated{id: "1", user_id: "j", name: "John", sleep: 100}
    ]

    InMemoryEventLog.append(:events, events)
    Derive.Dispatcher.await(dispatcher, events)

    Process.exit(dispatcher, :normal)
    # wait for the process to exit
    Process.sleep(1)
    assert Process.alive?(dispatcher) == false

    # Append some events while the dispatcher is dead
    events = [
      %UserNameUpdated{id: "2", user_id: "j", name: "John Smith", sleep: 100}
    ]

    InMemoryEventLog.append(:events, events)

    # Dispatcher should pick up where it left off and process the remaining events
    {:ok, dispatcher} = Derive.Dispatcher.start_link(UserReducer)

    Derive.Dispatcher.await(dispatcher, events)

    # john = Derive.Repo.get(User, "j")
    # assert john.name == "John Smith"
  end
end
