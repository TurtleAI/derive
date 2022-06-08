defmodule Derive.Ecto.ServiceTest do
  use ExUnit.Case

  alias DeriveTestRepo, as: Repo
  alias Derive.Timespan
  alias Derive.Logger.InMemoryLogger
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
      FakeEventLog.persist(events)
      Derive.MultiOp.committed(op)
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

    FakeEventLog.reset_state()

    :ok
  end

  def concurrent?(t1, t2),
    do: Timespan.overlaps?(t1, t2) == true

  def sequential?(t1, t2),
    do: Timespan.overlaps?(t1, t2) == false

  def failed_multis(logger) do
    InMemoryLogger.fetch(logger)
    |> Enum.flat_map(fn
      {:error, {:multi_op, multi}} -> [multi]
      _ -> []
    end)
  end

  test "when there is a failure, the partition should be marked in an error state" do
    name = :process_entries_sequentially

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

    assert [%Event{data: %{"amount" => 25}, id: "event-1"}] = events

    Derive.stop(name)
  end
end
