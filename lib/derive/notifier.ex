defmodule Derive.Notifier do
  @moduledoc """
  A notifier pattern for a reducer for things like:
  - Sending realtime events
  - Sending email notifications
  - Sending push notifications

  ## Example

      defmodule UserReducer do
        use Derive.Notifier

        @impl true
        def partition(%{user_id: user_id}), do: user_id

        @impl true
        def handle_event(%UserCreated{user_id: user_id, name: name, email: email}) do
          %UserRegisteredNotification{user_id: user_id}
        end

        @impl true
        def commit(%MultiOp{}=multi) do
          deliver_emails(multi)
        end
      end
  """

  alias Derive.Partition

  @doc """
  When booting, get the cursor for the global partition to resume operations
  """
  @callback load_initial_cursor(Derive.Options.t()) :: Derive.Reducer.cursor()

  @doc """
  For a given event, return a operation that should be run as a result.
  This is usually for keeping state up to date.

  How the operation is processed depends on the sink.
  """
  @callback commit(Derive.State.MultiOp.t()) :: Derive.State.MultiOp.t()

  @doc false
  def partition_map(cursor) do
    %{Partition.global_id() => %Partition{id: Partition.global_id(), status: :ok, cursor: cursor}}
  end

  defmacro __using__(_opts) do
    quote do
      use Derive.Reducer
      @behaviour Derive.Notifier

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
      def load_partition(%Derive.Options{name: name}, id) do
        Derive.State.InMemory.PartitionRepo.load_partition(:"#{name}.partition_repo", id)
      end

      @impl true
      def save_partition(%Derive.Options{name: name}, partition) do
        Derive.State.InMemory.PartitionRepo.save_partition(:"#{name}.partition_repo", partition)
      end

      @impl true
      def child_specs(%Derive.Options{name: name} = options) do
        [
          {Derive.State.InMemory.PartitionRepo,
           name: :"#{name}.partition_repo",
           load_initial_partitions: fn ->
             Derive.Notifier.partition_map(load_initial_cursor(options))
           end}
        ]
      end
    end
  end
end
