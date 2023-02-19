defmodule Derive.Logger.InMemoryLogger do
  @moduledoc """
  An ephemeral in-memory implementation of `Derive.EventLog`
  Used only for test
  """

  use GenServer

  @type t :: %__MODULE__{
          messages: [any()]
        }
  defstruct messages: []

  alias __MODULE__, as: S

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, %S{}, opts)

  ### Client
  @doc """
  Return all of the message of a particular type
  """
  def messages(server, type),
    do: GenServer.call(server, {:messages, type})

  ### Server

  @impl true
  def init(state),
    do: {:ok, state}

  @impl true
  def handle_call(
        {:messages, type},
        _from,
        %S{messages: messages} = state
      ) do
    messages = for {^type, message} <- Enum.reverse(messages), do: message
    {:reply, messages, state}
  end

  def handle_call(
        :flush,
        _from,
        state
      ) do
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast(
        {:log, message},
        %S{messages: messages} = state
      ) do
    {:noreply, %{state | messages: [message | messages]}}
  end

  @impl true
  def handle_cast({:log, _}, state),
    do: {:noreply, state}
end
