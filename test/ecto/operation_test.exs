defmodule Derive.Ecto.OperationTest do
  use ExUnit.Case

  alias DeriveTestRepo, as: Repo

  import Derive.Ecto.Operation

  defmodule Person do
    use Derive.Ecto.Model

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

  defmodule Checkin do
    use Derive.Ecto.Model

    @primary_key false
    schema "checkins" do
      field(:user_id, :string, primary_key: true)
      field(:location_id, :string, primary_key: true)
      field(:timestamp, :utc_datetime)
    end

    def up do
      create table(:checkins, primary_key: false) do
        add(:user_id, :string, size: 32, primary_key: true)
        add(:location_id, :string, size: 32, primary_key: true)
        add(:timestamp, :utc_datetime)
      end
    end

    def down, do: drop_if_exists(table(:checkins))
  end

  defmodule FriendRequest do
    use Derive.Ecto.Model

    @primary_key false
    schema "friend_requests" do
      field(:user_id, :string, primary_key: true)
      field(:friend_id, :string, primary_key: true)
    end

    def up do
      create table(:friend_requests, primary_key: false) do
        add(:user_id, :string, size: 32, primary_key: true)
        add(:friend_id, :string, size: 32, primary_key: true)
      end
    end

    def down, do: drop_if_exists(table(:friend_requests))
  end

  setup_all do
    Repo.start_link()

    state = %Derive.Ecto.State{
      repo: Repo,
      namespace: "db_op_test",
      models: [Person, Checkin, FriendRequest]
    }

    Derive.Ecto.State.reset_state(state)

    :ok
  end

  setup do
    # Explicitly get a connection before each test
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)

    # Setting the shared mode must be done only after checkout
    Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})

    :ok
  end

  def to_multi(multi, _, []), do: multi

  def to_multi(multi, index, [op | rest]) do
    multi
    |> Ecto.Multi.append(Derive.Ecto.DbOp.to_multi(op, index))
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

    assert %{name: "John"} = Repo.get(Person, "1")
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

    # update with no fields is a noop
    commit([
      update({Person, "o"}, []),
      update({Person, "o"}, %{})
    ])
  end

  test "insert_new" do
    commit([
      insert_new(%Person{id: "x", name: "Xavier"}),
      insert_new(%Person{id: "x", name: "Xavierrrrryyyy"})
    ])

    assert %{name: "Xavier"} = Repo.get(Person, "x")
  end

  test "upsert" do
    commit([
      insert(%Checkin{
        user_id: "a",
        location_id: "x",
        timestamp: ~U[2022-04-29T15:00:00Z]
      }),
      upsert(
        %Checkin{
          user_id: "a",
          location_id: "x",
          timestamp: ~U[2023-12-12T15:00:00Z]
        },
        set: [timestamp: ~U[2099-12-12T15:00:00Z]]
      )
    ])

    assert %{user_id: "a", location_id: "x", timestamp: ~U[2099-12-12T15:00:00Z]} =
             Repo.get_by(Checkin, user_id: "a", location_id: "x")
  end

  test "merge" do
    commit([
      insert(%Person{id: "3", name: "Bruce", email: "bruce@hotmail.com"}),
      merge({Person, "3"}, name: "Wayney")
    ])

    assert %{name: "Wayney", email: "bruce@hotmail.com"} = Repo.get(Person, "3")
  end

  describe "replace" do
    test "single pk" do
      commit([
        replace(%Person{id: "r", name: "Robin", email: "rob@hotmail.com"}),
        replace(%Person{id: "r", name: "Robber"})
      ])

      # The entire record gets replaced based on the primary key
      assert %{id: "r", name: "Robber", email: nil, age: 0} = Repo.get(Person, "r")
    end

    test "composite pk" do
      commit([
        replace(%Checkin{
          user_id: "a",
          location_id: "x",
          timestamp: ~U[2022-04-29T15:00:00Z]
        }),
        replace(%Checkin{
          user_id: "a",
          location_id: "x",
          timestamp: ~U[2023-12-12T15:00:00Z]
        })
      ])

      assert %{user_id: "a", location_id: "x", timestamp: ~U[2023-12-12T15:00:00Z]} =
               Repo.get_by(Checkin, user_id: "a", location_id: "x")
    end

    test "pk with no other fields" do
      # when no other fields are present, we should prevent a stray replace statement
      commit([
        replace(%FriendRequest{
          user_id: "a",
          friend_id: "b"
        })
      ])

      assert %{user_id: "a", friend_id: "b"} =
               Repo.get_by(FriendRequest, user_id: "a", friend_id: "b")
    end
  end

  test "inc" do
    commit([
      insert(%Person{id: "4", name: "Flash", age: 35}),
      inc({Person, "4"}, :age, 2),
      inc({Person, "4"}, :age, -10)
    ])

    assert %{age: 27} = Repo.get(Person, "4")
  end

  test "transaction" do
    commit([
      insert(%Person{id: "r", name: "Robinhood", age: 99}),
      transaction(fn repo ->
        user = repo.get(Person, "r")
        update({Person, "r"}, name: "**" <> user.name <> "**")
      end),
      inc({Person, "r"}, :age, 1)
    ])

    assert %{name: "**Robinhood**", age: 100} = Repo.get(Person, "r")
  end
end
