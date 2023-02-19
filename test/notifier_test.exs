defmodule Derive.NotifierTest do
  use ExUnit.Case

  alias Derive.MultiOp
  alias Derive.EventLog.InMemoryEventLog, as: EventLog

  defmodule UserCreated do
    defstruct [:id, :user_id, :name, :email]
  end

  defmodule UserNameUpdated do
    defstruct [:id, :user_id, :name]
  end

  defmodule Email do
    defstruct [:to, :message]
  end

  defmodule EmailServer do
    def start_link(name),
      do: Agent.start_link(fn -> [] end, name: name)

    def send_email(server, email),
      do: Agent.update(server, fn emails -> emails ++ [email] end)

    def get_emails(email_server),
      do: Agent.get(email_server, fn emails -> emails end)
  end

  defmodule UserNotifier do
    use Derive.Notifier

    @impl true
    def partition(%{user_id: user_id}), do: user_id

    @impl true
    def handle_event(%UserCreated{user_id: user_id, name: :error}) do
      raise "error #{user_id}"
    end

    def handle_event(%UserCreated{user_id: user_id, name: name}) do
      %Email{to: user_id, message: "Hi welcome #{name}"}
    end

    def handle_event(%UserNameUpdated{user_id: user_id, name: name}) do
      %Email{to: user_id, message: "Hi you changed your name to #{name}"}
    end

    @impl true
    def commit(%MultiOp{} = op) do
      emails = Derive.MultiOp.operations(op)

      for email <- emails do
        EmailServer.send_email(:emails, email)
      end

      MultiOp.committed(op)
    end

    @impl true
    def load_initial_cursor(_),
      do: Agent.get(:cursor, fn x -> x end)
  end

  setup do
    {:ok, email_server} = EmailServer.start_link(:emails)
    {:ok, cursor} = Agent.start_link(fn -> "0" end, name: :cursor)

    {:ok, %{email_server: email_server, cursor: cursor}}
  end

  test "sends notifications", %{email_server: email_server} do
    name = :basic_notifications

    {:ok, event_log} = EventLog.start_link()

    {:ok, _} = Derive.start_link(reducer: UserNotifier, source: event_log, name: name)

    EventLog.append(event_log, [
      %UserCreated{id: "1", user_id: 99, name: "John"},
      %UserNameUpdated{id: "2", user_id: 99, name: "Pikachu"}
    ])

    Derive.await(name, [
      %UserCreated{id: "1", user_id: 99, name: "John"},
      %UserNameUpdated{id: "2", user_id: 99, name: "Pikachu"}
    ])

    assert [
             %Derive.NotifierTest.Email{message: "Hi welcome John", to: 99},
             %Derive.NotifierTest.Email{message: "Hi you changed your name to Pikachu", to: 99}
           ] = EmailServer.get_emails(email_server)

    Derive.stop(name)
  end

  test "skips over already sent notifications", %{email_server: email_server, cursor: cursor} do
    name = :skip_notifications

    Agent.update(cursor, fn _ -> "1" end)

    {:ok, event_log} = EventLog.start_link()

    {:ok, _} = Derive.start_link(reducer: UserNotifier, source: event_log, name: name)

    EventLog.append(event_log, [
      %UserCreated{id: "1", user_id: 99, name: "John"},
      %UserNameUpdated{id: "2", user_id: 99, name: "Pikachu"}
    ])

    Derive.await(name, [
      %UserCreated{id: "1", user_id: 99, name: "John"},
      %UserNameUpdated{id: "2", user_id: 99, name: "Pikachu"}
    ])

    assert [
             %Email{message: "Hi you changed your name to Pikachu", to: 99}
           ] = EmailServer.get_emails(email_server)

    Derive.stop(name)
  end

  test "if a notification has an error in handle_event, the error is logged" do
    name = :notifier_error

    {:ok, event_log} = EventLog.start_link()

    {:ok, logger} = Derive.Logger.InMemoryLogger.start_link()

    {:ok, _} =
      Derive.start_link(reducer: UserNotifier, source: event_log, logger: logger, name: name)

    EventLog.append(event_log, [
      %UserCreated{id: "1", user_id: 99, name: :error}
    ])

    Derive.await(name, [
      %UserCreated{id: "1", user_id: 99, name: :error}
    ])

    [error] = Derive.Logger.InMemoryLogger.messages(logger, :error)

    assert %Derive.MultiOp{
             error: %Derive.Error.HandleEventError{
               error: %RuntimeError{message: "error 99"},
               event: %Derive.NotifierTest.UserCreated{
                 email: nil,
                 id: "1",
                 name: :error,
                 user_id: 99
               },
               operation: %Derive.EventOp{
                 cursor: "1",
                 error: {%RuntimeError{message: "error 99"}, [_ | _]},
                 event: %Derive.NotifierTest.UserCreated{
                   email: nil,
                   id: "1",
                   name: :error,
                   user_id: 99
                 },
                 operations: [],
                 status: :error,
                 timespan: {{_, _, _}, {_, _, _}}
               },
               stacktrace: [_ | _]
             },
             initial_partition: %Derive.Partition{cursor: :start, error: nil, id: 99, status: :ok},
             partition: %Derive.Partition{
               cursor: :start,
               error: %Derive.PartitionError{batch: [], cursor: "1", type: :handle_event},
               id: 99,
               status: :error
             },
             save_partition: nil,
             status: :error
           } = error

    Derive.stop(name)
  end
end
