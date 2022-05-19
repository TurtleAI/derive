defmodule Derive.State.EctoTest do
  use ExUnit.Case

  alias DeriveTestRepo, as: Repo

  alias Derive.State.Ecto, as: EctoState
  alias Derive.Partition

  defmodule Fruit do
    use Derive.State.Ecto.Model

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

  @state %EctoState{repo: Repo, models: [Fruit], namespace: "fruits"}

  test "init state, then clear state" do
    EctoState.init_state(@state)
    assert {:ok, []} = get_rows()

    EctoState.clear_state(@state)
    assert {:error, :undefined_table} == get_rows()
  end

  test "clear state doesn't crash" do
    EctoState.clear_state(@state)
    assert {:error, :undefined_table} == get_rows()
  end

  test "init state, insert rows, then reset state" do
    EctoState.init_state(@state)
    Repo.insert(%Fruit{id: "1", name: "apple"})

    assert {:ok, [%Fruit{id: "1", name: "apple"}]} = get_rows()

    EctoState.reset_state(@state)
    assert {:ok, []} = get_rows()
    EctoState.clear_state(@state)
  end

  describe "get/set partitions" do
    test "can set and get back a partition" do
      EctoState.init_state(@state)

      EctoState.set_partition(@state, %Partition{id: "x", status: :ok, cursor: "1"})
      %Partition{id: "x", status: :ok, cursor: "1"} = EctoState.get_partition(@state, "x")

      EctoState.set_partition(@state, %Partition{id: "y", status: :error, error: %{"type" => "handle_error", "message" => "foo foo"}, cursor: "2"})
      %Partition{id: "y", status: :error, cursor: "2", error: %{"type" => "handle_error", "message" => "foo foo"}} = EctoState.get_partition(@state, "y")

      EctoState.clear_state(@state)
    end
  end

  describe "versioning" do
    test "the state defaults to 1" do
      EctoState.init_state(@state)
      assert %Partition{cursor: "1"} = EctoState.get_partition(@state, Partition.version_id())
      EctoState.clear_state(@state)
    end

    test "the state version can be overridden, then invalidated" do
      state = %{@state | version: "1.1"}
      EctoState.init_state(%{state | version: "1.1"})

      assert %Partition{cursor: "1.1"} = EctoState.get_partition(state, Partition.version_id())

      assert EctoState.needs_rebuild?(state) == false
      assert EctoState.needs_rebuild?(%{state | version: "1.2"}) == true

      EctoState.clear_state(state)
    end

    test "rebuild needed when there is nothing present" do
      assert EctoState.needs_rebuild?(@state) == true
    end
  end
end
