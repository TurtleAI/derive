defmodule DeriveEctoTest do
  use ExUnit.Case

  alias Derive.EventLog.InMemoryEventLog, as: EventLog

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

  defmodule UserRaiseError do
    defstruct [:id, :user_id, :message]
  end

  defmodule UserError do
    defexception [:message]
  end

  defmodule UserReducer do
    use Derive.Reducer

    import Derive.State.Ecto.Operation

    def partition(%{user_id: user_id}), do: user_id

    def models,
      do: [User, LogEntry]

    @state %Derive.State.Ecto{repo: Derive.Repo, namespace: "user_reducer"}

    defp maybe_sleep(0), do: :ok
    defp maybe_sleep(timeout), do: Process.sleep(timeout)

    defp log(message) do
      insert(%LogEntry{message: message, timestamp: :os.system_time(:millisecond)})
    end

    def on_error, do: :halt

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

    def handle_event(%UserRaiseError{message: message}) do
      raise UserError, message
    end

    def commit_operations(op),
      do: Derive.State.Ecto.commit(@state, op)

    def process_events(events, partition),
      do: Derive.Util.process_events(events, __MODULE__, partition)

    def get_partition(id),
      do: Derive.State.Ecto.get_partition(@state, id)

    def set_partition(partition),
      do: Derive.State.Ecto.set_partition(@state, partition)

    def reset_state,
      do: Derive.State.Ecto.reset_state(@state, models())
  end

  setup_all do
    {:ok, _pid} = Derive.Repo.start_link()

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

    :ok
  end

  test "insert a user" do
    name = :insert_a_user

    {:ok, event_log} = EventLog.start_link()
    {:ok, _} = Derive.start_link(reducer: UserReducer, source: event_log, name: name)

    EventLog.append(event_log, [
      %UserCreated{id: "1", user_id: "99", name: "John"}
    ])

    Derive.await(name, [
      %UserCreated{id: "1", user_id: "99", name: "John"}
    ])

    user = Derive.Repo.get(User, "99")
    assert user.name == "John"

    EventLog.append(event_log, [
      %UserNameUpdated{id: "2", user_id: "99", name: "John Wayne"}
    ])

    Derive.await(name, [
      %UserNameUpdated{id: "2", user_id: "99", name: "John Wayne"}
    ])

    user = Derive.Repo.get(User, "99")
    assert user.name == "John Wayne"
  end

  test "events are processed in parallel according to the partition" do
    name = :parallel

    {:ok, event_log} = EventLog.start_link()
    {:ok, _derive} = Derive.start_link(name: name, reducer: UserReducer, source: event_log)

    events = [
      %UserCreated{id: "1", user_id: "s", name: "Same", sleep: 100},
      %UserNameUpdated{id: "2", user_id: "s", name: "Similar", sleep: 100},
      %UserCreated{id: "3", user_id: "t", name: "Time", sleep: 100}
    ]

    EventLog.append(event_log, events)
    Derive.await(name, events)

    assert [{"created-s", t1}, {"created-t", t2}, {"updated-s", t3}] = get_logs()

    assert t2 - t1 <= @same_time_threshold

    assert t3 - t2 > @same_time_threshold

    [same, time] = [Derive.Repo.get(User, "s"), Derive.Repo.get(User, "t")]

    assert same.name == "Similar"
    assert time.name == "Time"
  end

  test "events are processed when there are more events than the batch size allows" do
    name = :batch_dispatcher

    {:ok, event_log} = EventLog.start_link()

    {:ok, _} =
      Derive.start_link(name: name, reducer: UserReducer, source: event_log, batch_size: 2)

    events = [
      %UserCreated{id: "1", user_id: "99", name: "Pear"},
      %UserNameUpdated{id: "2", user_id: "99", name: "Blueberry"},
      %UserNameUpdated{id: "3", user_id: "99", name: "Apple"},
      %UserNameUpdated{id: "4", user_id: "99", name: "Orange"},
      %UserNameUpdated{id: "5", user_id: "99", name: "Mango"}
    ]

    EventLog.append(event_log, events)
    Derive.await(name, events)

    user = Derive.Repo.get(User, "99")
    assert user.name == "Mango"
  end

  @tag :focus
  test "a partition is halted if an error is raised in handle_event" do
    name = :partition_halted

    {:ok, event_log} = EventLog.start_link()
    {:ok, _} = Derive.start_link(name: name, reducer: UserReducer, source: event_log)

    event = %UserCreated{id: "1", user_id: "99", name: "Pikachu"}

    EventLog.append(event_log, [event])

    Derive.await(name, [event])

    events = [
      %UserCreated{id: "2", user_id: "55", name: "Squirtle"},
      %UserRaiseError{id: "3", user_id: "99", message: "bad stuff happened"},
      %UserNameUpdated{id: "4", user_id: "99", name: "Raichu"},
      %UserNameUpdated{id: "5", user_id: "55", name: "Wartortle"}
    ]

    EventLog.append(event_log, events)
    Derive.await(name, events)

    user = Derive.Repo.get(User, "99")
    assert user.name == "Pikachu"

    # other partitions can happily continue processing
    user = Derive.Repo.get(User, "55")
    assert user.name == "Wartortle"

    # future events are not processed after a failure
    events = [%UserNameUpdated{id: "6", user_id: "99", name: "Super Pikachu"}]
    EventLog.append(event_log, events)
    Derive.await(name, events)

    # name hasn't changed
    user = Derive.Repo.get(User, "99")
    assert user.name == "Pikachu"
  end

  test "resuming a dispatcher after a server is restarted" do
    name = :resuming

    {:ok, event_log} = EventLog.start_link()
    {:ok, derive} = Derive.start_link(name: name, reducer: UserReducer, source: event_log)

    events = [
      %UserCreated{id: "1", user_id: "j", name: "John", sleep: 100}
    ]

    EventLog.append(event_log, events)
    Derive.await(name, events)

    Process.monitor(derive)
    Process.exit(derive, :normal)

    receive do
      {:DOWN, _ref, :process, ^derive, _} ->
        :ok
    end

    assert Process.alive?(derive) == false

    # Append some events while the dispatcher is dead
    events = [
      %UserNameUpdated{id: "2", user_id: "j", name: "John Smith", sleep: 100}
    ]

    EventLog.append(event_log, events)

    # Dispatcher should pick up where it left off and process the remaining events
    {:ok, _} = Derive.start_link(name: name, reducer: UserReducer, source: event_log)

    # TODO: remove this hack
    Process.sleep(50)

    Derive.await(name, events)

    john = Derive.Repo.get(User, "j")
    assert john.name == "John Smith"
  end

  test "rebuilding the state for a reducer" do
    name = :rebuild

    {:ok, event_log} = EventLog.start_link()
    {:ok, derive} = Derive.start_link(name: name, reducer: UserReducer, source: event_log)

    events = [
      %UserCreated{id: "1", user_id: "99", name: "John"},
      %UserNameUpdated{id: "2", user_id: "99", name: "John Wayne"}
    ]

    EventLog.append(event_log, events)
    Derive.await(name, events)

    user = Derive.Repo.get(User, "99")
    assert user.name == "John Wayne"

    Derive.Repo.delete_all(User)

    Process.monitor(derive)
    Process.exit(derive, :normal)

    receive do
      {:DOWN, _ref, :process, ^derive, _} ->
        :ok
    end

    Derive.rebuild(UserReducer, source: event_log)

    # user = Derive.Repo.get(User, "99")
    # assert user.name == "John Wayne"
  end
end
