defmodule Derive.Ecto.ServiceTest do
  use ExUnit.Case

  alias DeriveTestRepo, as: Repo
  alias Derive.Timespan
  alias Derive.EventLog.InMemoryEventLog, as: EventLog

  defmodule Event do
    use Derive.Ecto.Model

    @primary_key {:id, :string, []}
    schema "events" do
      field(:data, :map)
    end

    def up do
      create table(:events, primary_key: false) do
        add(:id, :string, size: 32, primary_key: true)
        add(:data, :map)
      end
    end

    def down, do: drop_if_exists(table(:events))
  end

  defmodule TimeTracked do
    defstruct [:id, :amount]
  end

  defmodule FakeEventLog do
    @state %Derive.Ecto.State{
      repo: Repo,
      models: [Event],
      namespace: "events"
    }

    def reset_state do
      Derive.Ecto.State.reset_state(@state)
    end

    def persist(events) do
      for e <- events do
        Repo.insert(e)
      end
    end
  end

  defmodule AccountingService do
    use Derive.Ecto.Service,
      repo: Repo,
      namespace: "accounting_service"

    @impl true
    def partition(%TimeTracked{}), do: "main"
    def partition(_), do: nil

    @impl true
    def handle_event(%TimeTracked{id: id, amount: amount}) do
      %Event{
        id: "event-#{id}",
        data: %{"amount" => amount}
      }
    end

    @impl true
    def commit(%Derive.MultiOp{} = op) do
      events = Derive.MultiOp.operations(op)

      if Enum.any?(events, fn
           %Event{data: %{"amount" => amount}} -> amount < 0
           _ -> false
         end) do
        Derive.MultiOp.commit_failed(op, {:negative_balance, nil})
      else
        FakeEventLog.persist(events)
        Derive.MultiOp.committed(op)
      end
    end

    @impl true
    def get_initial_cursor(_),
      do: "0"
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

    FakeEventLog.reset_state()

    :ok
  end

  def concurrent?(t1, t2),
    do: Timespan.overlaps?(t1, t2) == true

  def sequential?(t1, t2),
    do: Timespan.overlaps?(t1, t2) == false

  test "processes things sequentially " do
    name = :sequential_processing

    {:ok, event_log} = EventLog.start_link()

    {:ok, _} =
      Derive.start_link(
        reducer: AccountingService,
        source: event_log,
        name: name
      )

    EventLog.append(event_log, [
      %TimeTracked{id: "1", amount: 111},
      %TimeTracked{id: "2", amount: 222}
    ])

    Derive.await(name, [
      %TimeTracked{id: "1", amount: 111},
      %TimeTracked{id: "2", amount: 222}
    ])

    assert %Derive.Partition{
             cursor: "2",
             error: nil,
             id: "main",
             status: :ok
           } = AccountingService.load_partition(nil, "main")

    assert [
             %Event{data: %{"amount" => 111}, id: "event-1"},
             %Event{data: %{"amount" => 222}, id: "event-2"}
           ] = Repo.all(Event)

    EventLog.append(event_log, [
      %TimeTracked{id: "3", amount: 333}
    ])

    Derive.await(name, [
      %TimeTracked{id: "3", amount: 333}
    ])

    assert [
             %Event{data: %{"amount" => 111}, id: "event-1"},
             %Event{data: %{"amount" => 222}, id: "event-2"},
             %Event{data: %{"amount" => 333}, id: "event-3"}
           ] = Repo.all(Event)

    # shut down the process, add some events, then see if they get processed
    Derive.stop(name)

    EventLog.append(event_log, [
      %TimeTracked{id: "4", amount: 444}
    ])

    # Sleep as a sanity check to ensure we pick back where we leave off
    Process.sleep(50)

    {:ok, _} =
      Derive.start_link(
        reducer: AccountingService,
        source: event_log,
        name: name
      )

    Derive.await_catchup(name)

    assert [
             %Event{data: %{"amount" => 111}, id: "event-1"},
             %Event{data: %{"amount" => 222}, id: "event-2"},
             %Event{data: %{"amount" => 333}, id: "event-3"},
             %Event{data: %{"amount" => 444}, id: "event-4"}
           ] = Repo.all(Event)

    # # append an event that will cause a commit error
    EventLog.append(event_log, [
      %TimeTracked{id: "5", amount: -100}
    ])

    assert {:error, replies} =
             Derive.await(name, [
               %TimeTracked{id: "5", amount: -100}
             ])

    assert {:error,
            %Derive.PartitionError{
              batch: ["5"],
              cursor: nil,
              message: "** (ErlangError) Erlang error: :negative_balance",
              type: :commit
            }} =
             Derive.Replies.get(
               replies,
               {:sequential_processing, %TimeTracked{id: "5", amount: -100}}
             )

    Derive.stop(name)
  end

  test "when there is a failure, the partition should be marked in an error state" do
    name = :failure_error_state

    {:ok, event_log} = EventLog.start_link()

    {:ok, _} =
      Derive.start_link(
        reducer: AccountingService,
        source: event_log,
        name: name
      )

    EventLog.append(event_log, [
      %TimeTracked{id: "1", amount: 25},
      %TimeTracked{id: "2", amount: {:non_serializable, :value}}
    ])

    Derive.await(name, [
      %TimeTracked{id: "1", amount: 25},
      %TimeTracked{id: "2", amount: {:non_serializable, :value}}
    ])

    %Derive.Partition{
      cursor: :start,
      error: %Derive.PartitionError{
        batch: ["1", "2"],
        # we can't correlate to where the error started
        cursor: nil,
        message: message,
        type: :commit
      },
      id: "main",
      status: :error
    } = AccountingService.load_partition(nil, "main")

    assert String.contains?(
             message,
             "Jason.Encoder not implemented for {:non_serializable, :value} "
           )

    events = Repo.all(Event)
    assert [%Event{data: %{"amount" => 25}, id: "event-1"}] = events

    # it doesn't process future events
    EventLog.append(event_log, [
      %TimeTracked{id: "3", amount: 100}
    ])

    Derive.await(name, [
      %TimeTracked{id: "3", amount: 100}
    ])

    assert %Derive.Partition{
             cursor: :start,
             error: %Derive.PartitionError{
               cursor: nil,
               batch: ["1", "2"],
               message: "** (" <> _,
               type: :commit
             },
             id: "main",
             status: :error
           } = AccountingService.load_partition(nil, "main")

    events = Repo.all(Event)
    assert [%Event{data: %{"amount" => 25}, id: "event-1"}] = events

    Derive.stop(name)
  end
end
