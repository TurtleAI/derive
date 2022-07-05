defmodule Derive.Ecto.StateTest do
  use ExUnit.Case

  alias DeriveTestRepo, as: Repo

  alias Derive.Ecto.State
  alias Derive.{Partition, PartitionError}

  defmodule Fruit do
    use Derive.Ecto.Model

    @primary_key {:id, :string, []}
    schema "fruits" do
      field(:name, :string)
    end

    def up do
      create table(:fruits, primary_key: false) do
        add(:id, :string, size: 32, primary_key: true)
        add(:name, :string, size: 32)
      end
    end

    def down, do: drop_if_exists(table(:fruits))
  end

  setup_all do
    Repo.start_link()
    :ok
  end

  setup do
    # Explicitly get a connection before each test
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
    # Setting the shared mode must be done only after checkout
    Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})

    :ok
  end

  def get_rows do
    try do
      {:ok, Repo.all(Fruit)}
    catch
      :error, %Postgrex.Error{postgres: %{code: code}} ->
        {:error, code}
    end
  end

  @state %State{repo: Repo, models: [Fruit], namespace: "fruits"}

  test "init state, then clear state" do
    State.init_state(@state)
    assert {:ok, []} = get_rows()

    State.clear_state(@state)
    assert {:error, :undefined_table} == get_rows()
  end

  test "clear state doesn't crash" do
    State.clear_state(@state)
    assert {:error, :undefined_table} == get_rows()
  end

  test "init state, insert rows, then reset state" do
    State.init_state(@state)
    Repo.insert(%Fruit{id: "1", name: "apple"})

    assert {:ok, [%Fruit{id: "1", name: "apple"}]} = get_rows()

    State.reset_state(@state)
    assert {:ok, []} = get_rows()
    State.clear_state(@state)
  end

  describe "get/set partitions" do
    test "can set and get back a partition" do
      State.init_state(@state, [
        %Partition{
          id: "o",
          cursor: "99"
        }
      ])

      State.save_partitions(@state, [%Partition{id: "x", status: :ok, cursor: "1"}])

      %Partition{id: "x", status: :ok, cursor: "1"} = State.load_partition(@state, "x")
      %Partition{id: "o", status: :ok, cursor: "99"} = State.load_partition(@state, "o")

      State.save_partitions(@state, [
        %Partition{
          id: "y",
          status: :error,
          error: %PartitionError{type: :handle_event, message: "foo foo", cursor: "1"},
          cursor: "2"
        }
      ])

      %Partition{
        id: "y",
        status: :error,
        cursor: "2",
        error: %PartitionError{type: :handle_event, message: "foo foo", cursor: "1"}
      } = State.load_partition(@state, "y")

      State.clear_state(@state)
    end
  end

  describe "versioning" do
    test "the state defaults to 1" do
      State.init_state(@state)
      assert %Partition{cursor: "1"} = State.load_partition(@state, Partition.version_id())
      State.clear_state(@state)
    end

    test "the state version can be overridden, then invalidated" do
      state = %{@state | version: "1.1"}
      State.init_state(%{state | version: "1.1"})

      assert %Partition{cursor: "1.1"} = State.load_partition(state, Partition.version_id())

      assert State.needs_rebuild?(state) == false
      assert State.needs_rebuild?(%{state | version: "1.2"}) == true

      State.clear_state(state)
    end

    test "rebuild needed when there is nothing present" do
      assert State.needs_rebuild?(@state) == true
    end
  end
end
