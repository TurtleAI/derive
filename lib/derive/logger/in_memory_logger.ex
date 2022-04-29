defmodule Derive.Logger.InMemoryLogger do
  @moduledoc """
  An ephemeral in-memory implementation of `Derive.EventLog`
  Currently only meant for testing purposes
  """

  use GenServer

  @type t :: %__MODULE__{
          multis: [Derive.State.MultiOp.t()]
        }
  defstruct multis: []

  alias __MODULE__, as: S

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, %S{}, opts)

  ### Server

  @impl true
  def init(state),
    do: {:ok, state}

  @impl true
  def handle_call(
        :fetch,
        _from,
        %S{multis: multis} = state
      ) do
    {:reply, Enum.reverse(multis), state}
  end

  @impl true
  def handle_cast(
        {:log, {:committed, multi_op}},
        %S{multis: multis} = state
      ) do
    new_multis = [multi_op | multis]
    {:noreply, %{state | multis: new_multis}}
  end
end
