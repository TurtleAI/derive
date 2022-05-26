defmodule Derive.NotifierTest do
  use ExUnit.Case

  alias Derive.State.MultiOp
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
    def handle_event(%UserCreated{user_id: user_id, name: name, email: email}) do
      %Email{to: user_id, message: "Hi welcome #{name}"}
    end

    def handle_event(%UserNameUpdated{user_id: user_id, name: name}) do
      %Email{to: user_id, message: "Hi you changed your name to #{name}"}
    end

    def commit(%MultiOp{} = op) do
      emails = Derive.State.MultiOp.operations(op)

      for email <- emails do
        EmailServer.send_email(:emails, email)
      end

      MultiOp.committed(op)
    end

    @impl true
    def get_partition(id) do
      %Derive.Partition{
        id: id,
        cursor: "0",
        status: :ok
      }
    end

    @impl true
    def set_partition(_partition), do: :ok
  end

  setup do
    {:ok, email_server} = EmailServer.start_link(:emails)

    {:ok, %{email_server: email_server}}
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
end
