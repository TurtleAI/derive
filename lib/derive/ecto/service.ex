defmodule Derive.Ecto.Service do
  @callback commit(Derive.State.MultiOp.t()) :: Derive.State.MultiOp.t()

  @doc """
  The initial global cursor to start processing events for the first time.
  In many cases, a service is not intended to process historical events.

  By returning up the initial cursor (for example, MyEventLog.last_cursor()), we can ensure
  a service starts at the end of the event log rather than from the beginning.
  """
  @callback get_initial_cursor(Derive.Options.t()) :: Derive.EventLog.cursor()

  defmacro __using__(opts) do
    repo = Keyword.fetch!(opts, :repo)
    namespace = Keyword.fetch!(opts, :namespace)

    quote do
      use Derive.Reducer
      @behaviour Derive.Ecto.Service

      @state %Derive.Ecto.State{
        repo: unquote(repo),
        namespace: unquote(namespace),
        models: [],
        version: "1"
      }

      @impl true
      def setup(%Derive.Options{} = opts) do
        Derive.Ecto.State.init_state(@state, [
          %Derive.Partition{
            id: Derive.Partition.global_id(),
            cursor: get_initial_cursor(opts)
          }
        ])
      end

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

      @impl true
      def get_cursor(%{id: id}),
        do: id

      @impl true
      def load_partition(_opts, id),
        do: Derive.Ecto.State.load_partition(@state, id)

      @impl true
      def save_partition(_opts, partition),
        do: Derive.Ecto.State.save_partitions(@state, [partition])
    end
  end
end
