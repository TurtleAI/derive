defmodule Derive do
  @moduledoc """
  Help with the infrastructure to maintain a state that is eventually consistent

  The topology is as follows:
  `Derive.Source` -> `Derive.Dispatcher` -> `Derive.Sink`

  A source can produce events, those get processed by a dispatcher, and persisted to the sink.

  In order to use it, one must create a module that uses `Derive.Reducer` like so:

  ```
  defmodule UserReducer do
    use Derive.Reducer

    def source, do: :events
    def partition(%{user_id: user_id}), do: user_id
    def sink, do: :users

    def handle(%UserCreated{user_id: user_id, name: name, email: email}) do
      merge([User, user_id], %User{id: user_id, name: name, email: email})
    end
    def handle(%UserNameUpdated{user_id: user_id, name: name}) do
      merge([User, user_id], %{name: name})
    end
    def handle(%UserEmailUpdated{user_id: user_id, email: email}) do
      merge([User, user_id], %{email: email})
    end
    def handle(%UserDeactivated{user_id: user_id}) do
      delete([User, user_id])
    end

    def handle_error(%Derive.Reducer.Error{} = error) do
      {:retry, Derive.Reducer.Error.events_without_failed_events(error)}
    end
  end
  ```
  """

end
