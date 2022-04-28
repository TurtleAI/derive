defmodule DeriveEctoDbOpTest do
  use ExUnit.Case

  import Derive.State.Ecto.Operation

  alias DeriveTestRepo, as: Repo

  defmodule Person do
    use Derive.State.Ecto.Model

    @primary_key {:id, :string, []}
    schema "people" do
      field(:name, :string)
      field(:email, :string)
      field(:age, :integer)
    end

    def up do
      create table(:people, primary_key: false) do
        add(:id, :string, size: 32, primary_key: true)
        add(:name, :string, size: 32)
        add(:email, :string, size: 32)
        add(:age, :integer, default: 0)
      end
    end

    def down, do: drop_if_exists(table(:people))
  end

  setup_all do
    {:ok, _pid} = Repo.start_link()

    state = %Derive.State.Ecto{repo: Repo, namespace: "db_op_test", models: [Person]}
    Derive.State.Ecto.reset_state(state)

    :ok
  end

  setup do
    # Explicitly get a connection before each test
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
    # Setting the shared mode must be done only after checkout
    Ecto.Adapters.SQL.Sandbox.mode(Repo, :auto)

    :ok
  end

  def to_multi(multi, _, []), do: multi

  def to_multi(multi, index, [op | rest]) do
    multi
    |> Ecto.Multi.append(Derive.State.Ecto.DbOp.to_multi(op, index))
    |> to_multi(index + 1, rest)
  end

  def commit(operations) when is_list(operations) do
    to_multi(Ecto.Multi.new(), 0, operations)
    |> Repo.transaction()
  end

  def commit(op) when is_struct(op), do: commit([op])

  test "insert" do
    commit([
      insert(%Person{id: "1", name: "John"})
    ])

    assert Repo.get(Person, "1").name == "John"
  end

  test "delete" do
    commit([
      insert(%Person{id: "a", name: "Apple"}),
      insert(%Person{id: "b", name: "Bear"}),
      delete({Person, "a"})
    ])

    assert Repo.get(Person, "a") == nil
    assert Repo.get(Person, "b") != nil
  end

  test "update" do
    commit([
      insert(%Person{id: "o", name: "Orange"}),
      update({Person, "o"}, name: "OOO", email: "oo@oo.com")
    ])

    assert %{name: "OOO", email: "oo@oo.com"} = Repo.get(Person, "o")
  end

  test "insert_new" do
    commit([
      insert_new(%Person{id: "x", name: "Xavier"}),
      insert_new(%Person{id: "x", name: "Xavierrrrryyyy"})
    ])

    assert Repo.get(Person, "x").name == "Xavier"
  end

  describe "merge" do
    test "merge by a simple selector" do
      commit([
        insert(%Person{id: "3", name: "Bruce", email: "bruce@hotmail.com"}),
        merge({Person, "3"}, name: "Wayney")
      ])

      user = Repo.get(Person, "3")
      assert user.name == "Wayney"
      assert user.email == "bruce@hotmail.com"
    end
  end

  test "inc" do
    commit([
      insert(%Person{id: "4", name: "Flash", age: 35}),
      inc({Person, "4"}, :age, 2)
    ])

    user = Repo.get(Person, "4")
    assert user.age == 37
  end
end
