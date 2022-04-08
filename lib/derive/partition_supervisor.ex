defmodule Derive.PartitionSupervisor do
  use Supervisor

  @moduledoc """
  Manages a pool of GenServers of `Derive.PartitionDispatcher`
  Each partition incrementally updates a single partition to its latest state.
  """

  @supervisor Derive.PartitionSupervisor
  @registry Derive.Registry

  def start_link(mod, opts \\ []) do
    Supervisor.start_link(__MODULE__, mod, opts)
  end

  def init(_) do
    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: @supervisor}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  @doc """
  Return the given `Derive.PartitionSupervisor` dispatcher for the given {reducer, partition} pair
  Processes are lazily started.
  - If a process is already alive, we will return the existing process.
  - If a process hasn't been started, it'll be started and returned.
  """
  def lookup_or_start({reducer, partition}) do
    key = {reducer, partition}

    case Registry.lookup(@registry, key) do
      [{pid, _}] ->
        pid

      [] ->
        case DynamicSupervisor.start_child(
               @supervisor,
               {Derive.PartitionDispatcher,
                [reducer: reducer, partition: partition, name: Derive.Registry.via(key)]}
             ) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end
    end
  end

  def workers do
    @supervisor
    |> DynamicSupervisor.which_children()
    |> Enum.map(fn {_, pid, :worker, _} -> pid end)
  end
end
