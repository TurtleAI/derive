defmodule Derive.State.InMemory.PartitionRepo do
  @moduledoc """
  An ephemeral in-memory GenServer to keep track of partitions
  """

  use GenServer

  alias Derive.Partition

  @type t :: %__MODULE__{
          partitions: %{Partition.id() => Partition.t()}
        }
  defstruct partitions: %{}

  @type server :: pid()

  alias __MODULE__, as: S

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, %S{}, opts)

  ### Client

  @doc """
  Get a partition from memory
  """
  @spec load_partition(server(), Derive.Partition.id()) :: Derive.Partition.t()
  def load_partition(server, partition_id),
    do: GenServer.call(server, {:load_partition, partition_id})

  @spec save_partition(server(), Derive.Partition.t()) :: :ok
  def save_partition(server, partition),
    do: GenServer.call(server, {:save_partition, partition})

  def get_state(server),
    do: GenServer.call(server, :get_state)

  ### Server

  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_call(
        {:load_partition, partition_id},
        _from,
        %S{partitions: partitions} = state
      ) do
    {:reply,
     Map.get(partitions, partition_id, %Partition{
       id: partition_id,
       cursor: :start,
       status: :ok
     }), state}
  end

  def handle_call(
        {:save_partition, %Partition{id: id} = partition},
        _from,
        %S{partitions: partitions} = state
      ) do
    {:reply, :ok, %S{state | partitions: Map.put(partitions, id, partition)}}
  end

  def handle_call(
        :get_state,
        _from,
        state
      ) do
    {:reply, :ok, state}
  end
end
