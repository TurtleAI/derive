defmodule Derive.EctoService do
  @callback commit(Derive.State.MultiOp.t()) :: Derive.State.MultiOp.t()

  defmacro __using__(opts) do
    repo = Keyword.fetch!(opts, :repo)
    namespace = Keyword.fetch!(opts, :namespace)

    quote do
      use Derive.Reducer
      @behaviour Derive.EctoService

      @state %Derive.State.Ecto{
        repo: unquote(repo),
        namespace: unquote(namespace),
        models: [],
        version: "1"
      }

      @impl true
      def setup(%Derive.Options{} = opts) do
        Derive.State.Ecto.init_state(@state)
      end

      @impl true
      def process_events(events, multi) do
        Derive.Reducer.EventProcessor.process_events(
          events,
          multi,
          %Derive.Reducer.EventProcessor.Options{
            handle_event: &handle_event/1,
            get_cursor: &get_cursor/1,
            commit: &commit/1,
            on_error: :halt
          }
        )
      end

      @impl true
      def get_cursor(%{id: id}),
        do: id

      @impl true
      def load_partition(_opts, id),
        do: Derive.State.Ecto.load_partition(@state, id)

      @impl true
      def save_partition(_opts, partition),
        do: Derive.State.Ecto.save_partition(@state, partition)
    end
  end
end
