defmodule DeriveEctoTest do
  use ExUnit.Case

  alias Derive.EventLog.InMemoryEventLog, as: EventLog

  @same_time_threshold 10

  alias DeriveTestRepo, as: Repo

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

  defmodule UserRaiseHandleError do
    defstruct [:id, :user_id, :message]
  end

  defmodule UserMissingFieldUpdated do
    defstruct [:id, :user_id]
  end

  defmodule UserError do
    defexception [:message]
  end

  defmodule UserReducer do
    # TODO: Extract ecto-specific implementation to a `use Derive.EctoReducer` to minimize boilerplate
    use Derive.Reducer

    import Derive.State.Ecto.Operation

    @state %Derive.State.Ecto{
      repo: Repo,
      namespace: "user_reducer",
      models: [User, LogEntry]
    }

    defp maybe_sleep(0), do: :ok
    defp maybe_sleep(timeout), do: Process.sleep(timeout)

    defp log(message) do
      insert(%LogEntry{message: message, timestamp: :os.system_time(:millisecond)})
    end

    @impl true
    def partition(%{user_id: user_id}), do: user_id

    @impl true
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
        update({User, user_id}, %{name: name})
      ]
    end

    def handle_event(%UserRaiseHandleError{message: message}) do
      raise UserError, message
    end

    def handle_event(%UserMissingFieldUpdated{user_id: user_id}) do
      update({User, user_id}, missing_field: "stuff")
    end

    @impl true
    def reduce_events(events, partition) do
      Derive.Reducer.EventProcessor.reduce_events(
        events,
        Derive.State.MultiOp.new(partition),
        &handle_event/1,
        on_error: :halt
      )
    end

    @impl true
    def processed_event?(%{version: version}, %{id: id}),
      do: version >= id

    @impl true
    def commit(op),
      do: Derive.State.Ecto.commit(@state, op)

    @impl true
    def reset_state,
      do: Derive.State.Ecto.reset_state(@state)

    @impl true
    def get_partition(id),
      do: Derive.State.Ecto.get_partition(@state, id)

    @impl true
    def set_partition(partition),
      do: Derive.State.Ecto.set_partition(@state, partition)
  end

  setup_all do
    {:ok, _pid} = Repo.start_link()

    :ok
  end

  def get_logs() do
    for %{message: message, timestamp: timestamp} <- Repo.all(LogEntry) do
      {message, timestamp}
    end
    |> Enum.sort_by(fn {message, timestamp} ->
      {timestamp, message}
    end)
  end

  setup do
    # Explicitly get a connection before each test
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
    # Setting the shared mode must be done only after checkout

    # For some reason, :auto causes some tests to lock up
    # Ecto.Adapters.SQL.Sandbox.mode(Repo, :auto)
    Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})

    :ok
  end

  test "insert a user" do
    name = :insert_a_user

    {:ok, event_log} = EventLog.start_link()
    Derive.rebuild(UserReducer, source: event_log)

    {:ok, _} = Derive.start_link(reducer: UserReducer, source: event_log, name: name)

    EventLog.append(event_log, [
      %UserCreated{id: "1", user_id: "99", name: "John"}
    ])

    Derive.await(name, [
      %UserCreated{id: "1", user_id: "99", name: "John"}
    ])

    user = Repo.get(User, "99")
    assert user.name == "John"

    EventLog.append(event_log, [
      %UserNameUpdated{id: "2", user_id: "99", name: "John Wayne"}
    ])

    Derive.await(name, [
      %UserNameUpdated{id: "2", user_id: "99", name: "John Wayne"}
    ])

    assert %{name: "John Wayne"} = Repo.get(User, "99")
  end

  test "events are processed in parallel according to the partition" do
    name = :parallel

    {:ok, event_log} = EventLog.start_link()
    Derive.rebuild(UserReducer, source: event_log)

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

    [same, time] = [Repo.get(User, "s"), Repo.get(User, "t")]

    assert same.name == "Similar"
    assert time.name == "Time"
  end

  test "events are processed when there are more events than the batch size allows" do
    name = :batch_dispatcher

    {:ok, event_log} = EventLog.start_link()
    Derive.rebuild(UserReducer, source: event_log)

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

    assert %{name: "Mango"} = Repo.get(User, "99")
  end

  describe "error handling" do
    test "a partition is halted if an error is raised in handle_event" do
      name = :partition_halted

      {:ok, event_log} = EventLog.start_link()
      Derive.rebuild(UserReducer, source: event_log)

      {:ok, _} = Derive.start_link(name: name, reducer: UserReducer, source: event_log)

      event = %UserCreated{id: "1", user_id: "99", name: "Pikachu"}

      EventLog.append(event_log, [event])

      Derive.await(name, [event])

      events = [
        %UserCreated{id: "2", user_id: "55", name: "Squirtle"},
        %UserRaiseHandleError{id: "3", user_id: "99", message: "bad stuff happened"},
        %UserNameUpdated{id: "4", user_id: "99", name: "Raichu"},
        %UserNameUpdated{id: "5", user_id: "55", name: "Wartortle"}
      ]

      EventLog.append(event_log, events)
      Derive.await(name, events)

      assert %{name: "Pikachu"} = Repo.get(User, "99")

      # other partitions can happily continue processing
      assert %{name: "Wartortle"} = Repo.get(User, "55")

      # future events are not processed after a failure
      events = [%UserNameUpdated{id: "6", user_id: "99", name: "Super Pikachu"}]
      EventLog.append(event_log, events)
      Derive.await(name, events)

      # name hasn't changed
      assert %{name: "Pikachu"} = Repo.get(User, "99")
    end

    test "a commit failing causes the partition to halt" do
      # a copy of the previous test except this failure happens on commit

      name = :commit_failed

      {:ok, event_log} = EventLog.start_link()
      Derive.rebuild(UserReducer, source: event_log)

      {:ok, _} = Derive.start_link(name: name, reducer: UserReducer, source: event_log)

      event = %UserCreated{id: "1", user_id: "99", name: "Pikachu"}

      EventLog.append(event_log, [event])

      Derive.await(name, [event])

      events = [
        %UserCreated{id: "2", user_id: "55", name: "Squirtle"},
        %UserMissingFieldUpdated{id: "3", user_id: "99"},
        %UserNameUpdated{id: "4", user_id: "99", name: "Raichu"},
        %UserNameUpdated{id: "5", user_id: "55", name: "Wartortle"}
      ]

      EventLog.append(event_log, events)
      Derive.await(name, events)

      assert %{name: "Pikachu"} = Repo.get(User, "99")

      # other partitions can happily continue processing
      assert %{name: "Wartortle"} = Repo.get(User, "55")

      # future events are not processed after a failure
      events = [%UserNameUpdated{id: "6", user_id: "99", name: "Super Pikachu"}]
      EventLog.append(event_log, events)
      Derive.await(name, events)

      # name hasn't changed
      assert %{name: "Pikachu"} = Repo.get(User, "99")
    end
  end

  test "resuming a dispatcher after a server is restarted" do
    name = :resuming

    {:ok, event_log} = EventLog.start_link()
    Derive.rebuild(UserReducer, source: event_log)

    {:ok, derive} = Derive.start_link(name: name, reducer: UserReducer, source: event_log)

    events = [
      %UserCreated{id: "1", user_id: "j", name: "John", sleep: 100}
    ]

    EventLog.append(event_log, events)
    Derive.await(name, events)

    Derive.stop(derive)

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

    assert %{name: "John Smith"} = Repo.get(User, "j")
  end

  describe "&Derive.rebuild/2" do
    test "first-time rebuild" do
      {:ok, event_log} = EventLog.start_link()

      events = [
        %UserCreated{id: "1", user_id: "99", name: "John"},
        %UserNameUpdated{id: "2", user_id: "99", name: "John Wayne"}
      ]

      EventLog.append(event_log, events)

      Derive.rebuild(UserReducer, source: event_log)

      assert %{name: "John Wayne"} = Repo.get(User, "99")
    end

    @tag :focus
    test "rebuilding the state for a reducer" do
      name = :rebuild

      {:ok, event_log} = EventLog.start_link()
      Derive.rebuild(UserReducer, source: event_log)

      {:ok, derive} = Derive.start_link(name: name, reducer: UserReducer, source: event_log)

      events = [
        %UserCreated{id: "1", user_id: "99", name: "John"},
        %UserNameUpdated{id: "2", user_id: "99", name: "John Wayne"}
      ]

      EventLog.append(event_log, events)
      Derive.await(name, events)

      assert %{name: "John Wayne"} = Repo.get(User, "99")

      Repo.delete_all(User)

      Derive.stop(derive)

      Process.sleep(500)

      Derive.rebuild(UserReducer, source: event_log)

      user = Repo.get(User, "99")
      assert user.name == "John Wayne"
    end
  end
end
