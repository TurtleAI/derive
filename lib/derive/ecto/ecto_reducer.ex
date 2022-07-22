defmodule Derive.Ecto.Reducer do
  @moduledoc """
  An Ecto-specific implementation of `Derive.Reducer`

  ## Example

      defmodule UserReducer do
        use Derive.EctoReducer,
          repo: Repo,
          namespace: "user_reducer",
          models: [User],
          version: "1.2"

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
    version = Keyword.get(opts, :version, "1")

    quote do
      use Derive.Reducer
      @behaviour Derive.ReducerState

      @state %Derive.Ecto.State{
        repo: unquote(repo),
        namespace: unquote(namespace),
        models: unquote(models),
        version: unquote(version)
      }

      import Derive.Ecto.Operation

      @impl true
      def process_events(events, multi, %Derive.Options{logger: logger}) do
        Derive.Reducer.EventProcessor.process_events(
          events,
          multi,
          %Derive.Reducer.EventProcessor.Options{
            handle_event: &handle_event/1,
            get_cursor: &get_cursor/1,
            commit: &commit/1,
            on_error: :halt,
            logger: logger
          }
        )
      end

      def commit(op),
        do: Derive.Ecto.State.commit(@state, op)

      @impl true
      def get_cursor(%{id: id}),
        do: id

      @impl true
      def reset_state,
        do: Derive.Ecto.State.reset_state(@state)

      @impl true
      def needs_rebuild?,
        do: Derive.Ecto.State.needs_rebuild?(@state)

      @impl true
      def load_partition(_, id),
        do: Derive.Ecto.State.load_partition(@state, id)

      @impl true
      def save_partition(_opts, %Derive.Partition{} = partition),
        do: Derive.Ecto.State.save_partitions(@state, [partition])
    end
  end
end
