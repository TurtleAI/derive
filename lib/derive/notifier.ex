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

  # @doc """
  # When booting, get the cursor to start at for a given partition.
  # """
  # @callback load_partition_cursor(Derive.Partition.id()) :: Derive.Reducer.cursor()

  @doc """
  For a given event, return a operation that should be run as a result.
  This is usually for keeping state up to date.

  How the operation is processed depends on the sink.
  """
  @callback commit(Derive.State.MultiOp.t()) :: Derive.State.MultiOp.t()

  defmacro __using__(_opts) do
    quote do
      use Derive.Reducer
      @behaviour Derive.Notifier

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
    end
  end
end
