# Derive

Derive provides the infrastructure for efficiently keeping derived views up to date based on a data source.
This is commonly used for event sourcing or redux patterns.

Views are eventually consistent because processing is done asynchronously.
On a first run, the reducer will catch up to the latest state.
When new events are added to the source, they are incrementally processed and sent to the sink for persistence.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `derive` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:derive, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/derive](https://hexdocs.pm/derive).

## Basic usage

```elixir
defmodule User do
  use Ecto.Schema

  @primary_key {:id, :string, []}
  schema "users" do
    field :name, :string
    field :email, :string
  end
end

defmodule UserCreated do
  defstruct [:id, :user_id, :name, :email]
end

defmodule UserNameUpdated do
  defstruct [:id, :user_id, :name]
end

defmodule UserEmailUpdated do
  defstruct [:id, :user_id, :email]
end

defmodule UserDeactivated do
  defstruct [:id, :user_id]
end

defmodule UserReducer do
  use Derive.Reducer

  alias MyApp.{Repo, EventStore, User}

  def source do
    {Derive.Ecto.EventStream,
      store: EventStore,
      stream: [type: [UserCreated, UserNameUpdated, UserEmailUpdated, UserDeactivated]]
    }
  end

  def partition(%{user_id: user_id}), do: user_id

  def sink do
    {Derive.Ecto.Sink, repo: Repo, tables: [User]}
  end

  def handle_event(%UserCreated{user_id: user_id, name: name, email: email}) do
    merge([User, user_id], %{name: name, email: email})
  end
  def handle_event(%UserNameUpdated{user_id: user_id, name: name}) do
    merge([User, user_id], %{name: name})
  end
  def handle_event(%UserEmailUpdated{user_id: user_id, email: email}) do
    merge([User, user_id], %{email: email})
  end
  def handle_event(%UserDeactivated{user_id: user_id, email: email}) do
    delete([User, user_id])
  end
end

# rebuild a reducer
defmodule Sandbox do
  alias Derive.Reducer

  def rebuild do
    {:ok, reducer} = Reducer.start_link(UserReducer, mode: :rebuild)
    Reducer.wait_for_catch_up(reducer)
  end

  def normal_start do
    {:ok, reducer} = Reducer.start_link(UserReducer, mode: :catchup)
  end
end
```
