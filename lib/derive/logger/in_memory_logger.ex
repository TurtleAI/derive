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
  Fetch all of the multis in the order they were committed
  """
  def fetch(server),
    do: GenServer.call(server, :fetch)

  ### Server

  @impl true
  def init(state),
    do: {:ok, state}

  @impl true
  def handle_call(
        :fetch,
        _from,
        %S{messages: messages} = state
      ) do
    {:reply, Enum.reverse(messages), state}
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
