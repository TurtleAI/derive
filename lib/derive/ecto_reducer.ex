defmodule Derive.EctoReducer do
  @moduledoc """
  An ecto-specific implementation of `Derive.Reducer`

  ## Example

      defmodule UserReducer do
        use Derive.EctoReducer,
          repo: Repo,
          namespace: "user_reducer",
          models: [User]

        @impl true
        def partition(%{user_id: user_id}), do: user_id

        @impl true
        def handle_event(%UserCreated{user_id: user_id, name: name, email: email}) do
          insert(%User{
            id: user_id,
            name: name,
            email: email
          })
        end

        def handle_event(%UserNameUpdated{user_id: user_id, name: name, sleep: sleep}) do
          update({User, user_id}, %{name: name})
        end
      end
  """

  defmacro __using__(opts) do
    repo = Keyword.fetch!(opts, :repo)
    namespace = Keyword.fetch!(opts, :namespace)
    models = Keyword.fetch!(opts, :models)

    quote do
      @behaviour Derive.Reducer

      @state %Derive.State.Ecto{
        repo: unquote(repo),
        namespace: unquote(namespace),
        models: unquote(models)
      }

      import Derive.State.Ecto.Operation

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
      def processed_event?(%Derive.Partition{cursor: cursor}, %{id: id}),
        do: cursor >= id

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
  end
end
