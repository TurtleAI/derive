defmodule Derive.Ecto.ReducerTest do
  use ExUnit.Case

  alias Derive.EventLog.InMemoryEventLog, as: EventLog
  alias DeriveTestRepo, as: Repo
  alias Derive.{Timespan, Partition, MultiOp, EventOp}
  alias Derive.Error.{HandleEventError, CommitError}
  alias Derive.Logger.InMemoryLogger

  defmodule User do
    use Derive.Ecto.Model

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

  defmodule NotPartitioned do
    defstruct [:id]
  end

  defmodule NotProcessed do
    defstruct [:id, :user_id]
  end

  defmodule UserReducer do
    use Derive.Ecto.Reducer,
      repo: Repo,
      namespace: "user_reducer",
      models: [User]

    defp maybe_sleep(0), do: :ok
    defp maybe_sleep(timeout), do: Process.sleep(timeout)

    @impl true
    def partition(%{user_id: user_id}), do: user_id
    def partition(_), do: nil

    @impl true
    def handle_event(%UserCreated{user_id: user_id, name: name, email: email, sleep: sleep}) do
      maybe_sleep(sleep)

      insert(%User{
        id: user_id,
        name: name,
        email: email
      })
    end

    def handle_event(%UserNameUpdated{user_id: user_id, name: name, sleep: sleep}) do
      maybe_sleep(sleep)
      update({User, user_id}, %{name: name})
    end

    def handle_event(%UserRaiseHandleError{message: message}) do
      raise UserError, message
    end

    def handle_event(%UserMissingFieldUpdated{user_id: user_id}) do
      update({User, user_id}, missing_field: "stuff")
    end

    def handle_event(_) do
      nil
    end
  end

  setup_all do
    {:ok, _pid} = Repo.start_link()

    :ok
  end

  setup do
    # Explicitly get a connection before each test
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
    # Setting the shared mode must be done only after checkout

    # For some reason, :auto causes some tests to lock up
    Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})

    :ok
  end

  def concurrent?(t1, t2),
    do: Timespan.overlaps?(t1, t2) == true

  def sequential?(t1, t2),
    do: Timespan.overlaps?(t1, t2) == false

  def failed_multis(logger) do
    InMemoryLogger.fetch(logger)
    |> Enum.flat_map(fn
      {:multi, %MultiOp{status: :error} = multi} -> [multi]
      _ -> []
    end)
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

    Derive.stop(name)
  end

  test "allow adding a source to be a child spec like {mod, opts} instead of an atom" do
    {:ok, event_log} = EventLog.start_link()
    Derive.rebuild(UserReducer, source: event_log)

    name = :child_source
    event_log_name = :child_source_log

    {:ok, _} =
      Derive.start_link(
        reducer: UserReducer,
        source: {EventLog, [name: event_log_name]},
        name: name
      )

    EventLog.append(event_log_name, [
      %UserCreated{id: "1", user_id: "d", name: "Didi"}
    ])

    Derive.await(name, [
      %UserCreated{id: "1", user_id: "d", name: "Didi"}
    ])

    assert %{name: "Didi"} = Repo.get(User, "d")

    Derive.stop(name)
  end

  test "events are processed in parallel according to the partition" do
    name = :parallel

    {:ok, logger} = Derive.Logger.InMemoryLogger.start_link()
    {:ok, event_log} = EventLog.start_link()
    Derive.rebuild(UserReducer, source: event_log)

    {:ok, _derive} =
      Derive.start_link(name: name, reducer: UserReducer, source: event_log, logger: logger)

    e1 = %UserCreated{id: "1", user_id: "s", name: "Same", sleep: 100}
    e2 = %UserNameUpdated{id: "2", user_id: "s", name: "Similar", sleep: 100}
    e3 = %UserCreated{id: "3", user_id: "t", name: "Time", sleep: 100}

    events = [e1, e2, e3]

    EventLog.append(event_log, events)
    Derive.await(name, events)

    event_ops_by_event =
      Derive.Logger.InMemoryLogger.fetch(logger)
      |> Enum.flat_map(fn
        {:multi, %MultiOp{status: :committed, operations: operations}} -> operations
        _ -> []
      end)
      |> Enum.group_by(& &1.event)

    [op1, op2, op3] = Enum.map(events, fn e -> hd(Map.get(event_ops_by_event, e)) end)

    # ops in different partitions happen concurrently
    assert concurrent?(op1.timespan, op3.timespan)
    # ops in the same partition happen sequentially
    assert sequential?(op1.timespan, op2.timespan)

    assert [%{name: "Similar"}, %{name: "Time"}] = [Repo.get(User, "s"), Repo.get(User, "t")]

    Derive.stop(name)
  end

  describe "Derive.await/2" do
    test "await before an event has been persisted" do
      name = :basic_await

      {:ok, event_log} = EventLog.start_link()
      Derive.rebuild(UserReducer, source: event_log)

      {:ok, _derive} = Derive.start_link(name: name, reducer: UserReducer, source: event_log)

      e1 = %UserCreated{id: "1", user_id: "22", name: "Fake name"}
      e2 = %UserNameUpdated{id: "2", user_id: "22", name: "Real name"}

      Task.async(fn ->
        Process.sleep(50)
        EventLog.append(event_log, [e1, e2])
      end)

      Derive.await(name, [e1, e2])

      assert %{id: "22", name: "Real name"} = Repo.get(User, "22")

      # A second await completes immediately
      assert :ok = Derive.await(name, [e1, e2])

      Derive.stop(name)
    end

    test "await when a handle_event fails" do
      name = :await_handle_event_fail

      {:ok, event_log} = EventLog.start_link()
      Derive.rebuild(UserReducer, source: event_log)

      {:ok, _} = Derive.start_link(reducer: UserReducer, source: event_log, name: name)

      e1 = %UserRaiseHandleError{id: "1", user_id: "ee"}

      EventLog.append(event_log, [e1])

      assert :ok = Derive.await(name, [e1])

      Derive.stop(name)
    end

    test "when handle_event does nothing, await completes immediately" do
      name = :await_handle_event_skipped

      {:ok, event_log} = EventLog.start_link()
      Derive.rebuild(UserReducer, source: event_log)

      {:ok, _} = Derive.start_link(reducer: UserReducer, source: event_log, name: name)

      e1 = %NotProcessed{id: "1", user_id: "ee"}

      EventLog.append(event_log, [e1])

      assert :ok = Derive.await(name, [e1])

      Derive.stop(name)
    end

    test "when an event isn't partitioned, await completes immediately" do
      name = :await_handle_partition_skipped

      {:ok, event_log} = EventLog.start_link()
      Derive.rebuild(UserReducer, source: event_log)

      {:ok, _} = Derive.start_link(reducer: UserReducer, source: event_log, name: name)

      e1 = %NotPartitioned{id: "1"}

      EventLog.append(event_log, [e1])

      assert :ok = Derive.await(name, [e1])

      Derive.stop(name)
    end

    test "Derive.await_catchup" do
      name = :await_caught_up

      {:ok, event_log} = EventLog.start_link()
      Derive.rebuild(UserReducer, source: event_log)

      {:ok, _} =
        Derive.start_link(
          name: name,
          reducer: UserReducer,
          source: event_log
        )

      events = [
        %UserCreated{id: "1", user_id: "99", name: "Pear"},
        %UserNameUpdated{id: "2", user_id: "99", name: "Blueberry"}
      ]

      EventLog.append(event_log, events)

      Derive.await_catchup(name)

      assert %{name: "Blueberry"} = Repo.get(User, "99")

      # should not freeze the next time around
      Derive.await_catchup(name)

      Derive.stop(name)
    end
  end

  test "events are processed when there are more events than the configured batch size" do
    name = :batch_dispatcher

    {:ok, event_log} = EventLog.start_link()
    Derive.rebuild(UserReducer, source: event_log)

    {:ok, _} =
      Derive.start_link(
        name: name,
        reducer: UserReducer,
        source: event_log,
        batch_size: 2
      )

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

    Derive.stop(name)
  end

  describe "error handling" do
    test "a partition is halted if an error is raised in handle_event" do
      name = :partition_halted

      {:ok, event_log} = EventLog.start_link()
      Derive.rebuild(UserReducer, source: event_log)

      {:ok, logger} = InMemoryLogger.start_link()

      {:ok, _} =
        Derive.start_link(name: name, reducer: UserReducer, source: event_log, logger: logger)

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

      # the error log shows up
      [failed_multi] = failed_multis(logger)

      assert %Derive.MultiOp{
               partition: %Derive.Partition{
                 cursor: "1",
                 id: "99",
                 status: :error,
                 error: %Derive.PartitionError{
                   type: :handle_event,
                   cursor: "3",
                   message: "** (Derive.Ecto.ReducerTest.UserError) bad stuff happened" <> stack
                 }
               },
               error: %HandleEventError{
                 operation: %Derive.EventOp{
                   cursor: "3",
                   error: {%UserError{message: "bad stuff happened"}, [_ | _]},
                   event: %UserRaiseHandleError{
                     id: "3",
                     message: "bad stuff happened",
                     user_id: "99"
                   },
                   operations: [],
                   status: :error
                 }
               },
               operations: [
                 # we failed on event 3, so this following event gets skipped
                 %Derive.EventOp{
                   cursor: "4",
                   error: nil,
                   event: %UserNameUpdated{
                     id: "4",
                     name: "Raichu",
                     user_id: "99"
                   },
                   operations: [],
                   status: :ignore
                 }
               ],
               status: :error
             } = failed_multi

      assert String.contains?(stack, "lib/derive")

      assert %{name: "Pikachu"} = Repo.get(User, "99")

      # other partitions can happily continue processing
      assert %{name: "Wartortle"} = Repo.get(User, "55")

      # future events are not processed after a failure
      events = [%UserNameUpdated{id: "6", user_id: "99", name: "Super Pikachu"}]
      EventLog.append(event_log, events)
      Derive.await(name, events)

      assert %Derive.Partition{cursor: "5", id: "55", status: :ok} =
               UserReducer.load_partition(nil, "55")

      assert %Derive.Partition{cursor: "1", id: "99", status: :error} =
               UserReducer.load_partition(nil, "99")

      assert %Derive.Partition{cursor: "6", status: :ok} =
               UserReducer.load_partition(nil, Partition.global_id())

      # name hasn't changed
      assert %{name: "Pikachu"} = Repo.get(User, "99")

      Derive.stop(name)
    end

    test "a partition skips over events in future updates to the event log" do
      name = :partition_halted_skips_future_events

      {:ok, event_log} = EventLog.start_link()
      Derive.rebuild(UserReducer, source: event_log)

      {:ok, _} = Derive.start_link(name: name, reducer: UserReducer, source: event_log)

      events = [
        %UserCreated{id: "1", user_id: "99", name: "Mondo Man"}
      ]

      EventLog.append(event_log, events)
      Derive.await(name, events)

      events = [
        %UserRaiseHandleError{id: "2", user_id: "99", message: "bad stuff happened"},
        %UserNameUpdated{id: "3", user_id: "99", name: "This should be skipped"}
      ]

      EventLog.append(event_log, events)
      Derive.await(name, events)

      events = [
        %UserNameUpdated{id: "4", user_id: "99", name: "Duder ignore this too"}
      ]

      EventLog.append(event_log, events)
      Derive.await(name, events)

      assert %{name: "Mondo Man"} = Repo.get(User, "99")

      Derive.stop(name)
    end

    test "a commit failing due to a constraint violation" do
      name = :constraint_violation

      {:ok, event_log} = EventLog.start_link()
      Derive.rebuild(UserReducer, source: event_log)

      {:ok, logger} = InMemoryLogger.start_link()

      {:ok, _} =
        Derive.start_link(name: name, reducer: UserReducer, source: event_log, logger: logger)

      events = [
        %UserCreated{id: "1", user_id: "m", name: "Mondo Man"},
        %UserCreated{id: "2", user_id: "m", name: "Mondo Manner"}
      ]

      EventLog.append(event_log, events)
      Derive.await(name, events)

      assert %Partition{
               cursor: :start,
               error: %Derive.PartitionError{
                 cursor: "2",
                 message: _,
                 type: :commit
               },
               id: "m",
               status: :error
             } = UserReducer.load_partition(nil, "m")

      [multi] = failed_multis(logger)

      assert %MultiOp{
               status: :error,
               error: %CommitError{
                 operation: %EventOp{cursor: "2", event: %{name: "Mondo Manner"}},
                 error: [
                   id:
                     {"has already been taken",
                      [constraint: :unique, constraint_name: "users_pkey"]}
                 ]
               }
             } = multi

      Derive.stop(name)
    end

    test "a commit failing with an Ecto.QueryError" do
      name = :commit_failed

      {:ok, event_log} = EventLog.start_link()
      Derive.rebuild(UserReducer, source: event_log)

      {:ok, logger} = InMemoryLogger.start_link()

      {:ok, _} =
        Derive.start_link(name: name, reducer: UserReducer, source: event_log, logger: logger)

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

      Derive.stop(name)

      assert %Derive.Partition{
               cursor: "1",
               error: %Derive.PartitionError{
                 # since this is a commit exception, we don't know the cursor that caused the failure
                 cursor: nil,
                 batch: ["3", "4"],
                 message: "** (Ecto.QueryError)" <> error,
                 type: :commit
               },
               id: "99",
               status: :error
             } = UserReducer.load_partition(nil, "99")

      assert String.contains?(error, "missing_field")
      # stacktrace is included in the message
      assert String.contains?(error, "lib/ecto/")

      Derive.stop(name)
    end

    test "events are skipped over if the processing succeeds, but the Dispatcher fails to update its pointer" do
      name = :skip_over_processed_events

      {:ok, event_log} = EventLog.start_link()
      Derive.rebuild(UserReducer, source: event_log)

      {:ok, _} = Derive.start_link(name: name, reducer: UserReducer, source: event_log)

      events = [
        %UserCreated{id: "3", user_id: "99", name: "Saiyan"},
        %UserNameUpdated{id: "4", user_id: "99", name: "Supersaiyan"}
      ]

      EventLog.append(event_log, events)
      Derive.await(name, events)

      Derive.stop(name)

      # We move the partition to some earlier value to simulate a shut-down before things are finished
      UserReducer.save_partition(nil, %Derive.Partition{
        id: Partition.global_id(),
        cursor: "2"
      })

      {:ok, _} = Derive.start_link(name: name, reducer: UserReducer, source: event_log)

      events = [%UserNameUpdated{id: "5", user_id: "99", name: "Superdupersaiyan"}]
      EventLog.append(event_log, events)
      Derive.await(name, events)

      assert %{name: "Superdupersaiyan"} = Repo.get(User, "99")

      Derive.stop(name)
    end

    test "a process fails to start if a rebuild is needed" do
      name = :test_rebuild_needed

      {:ok, event_log} = EventLog.start_link()

      # The tables haven't been created yet, so trying to start a Derive process would fail
      assert {:error, {:needs_rebuild, UserReducer}} =
               Derive.start_link(
                 name: name,
                 reducer: UserReducer,
                 source: event_log,
                 validate_version: true
               )

      Derive.stop(name)
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

    # todo: remove this hack
    Process.sleep(1)

    Derive.await(name, events)

    assert %{name: "John Smith"} = Repo.get(User, "j")

    Derive.stop(name)
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

      Derive.rebuild(UserReducer, source: event_log)

      user = Repo.get(User, "99")
      assert user.name == "John Wayne"
    end
  end
end
