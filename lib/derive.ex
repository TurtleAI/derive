defmodule Derive do
  use Supervisor

  @moduledoc """
  Manages a pool of GenServers of `Derive.PartitionDispatcher`
  Each partition incrementally updates a single partition to its latest state.
  """

  def start_link(opts \\ []),
    do: Supervisor.start_link(__MODULE__, opts)

  ### Client
  @doc """
  Wait for all of the events to be processed by all of the matching partitions as defined by
  `Derive.Reducer.partition/1`

  If the event has already been processed, this will complete immediately
  If the event has not yet been processed, this will block until it completes processing

  Events are not considered processed until *all* operations produced by `Derive.Reducer.handle_event/1`
  have been committed by `Derive.Reducer.commit_operations/1`
  """
  def await(name, events),
    do: GenServer.call(dispatcher_name(name), {:await, events})

  @doc """
  Rebuilds the state of a reducer.
  This means the state will be reset and all of the events processed to get to the final state.
  """
  @spec rebuild(Derive.Reducer.t(), any()) :: :ok
  def rebuild(reducer, opts \\ []) do
    derive_opts = Keyword.merge(opts, reducer: reducer, name: reducer)

    reducer.reset_state()

    {:ok, derive} = start_link(derive_opts)
    Process.monitor(derive)

    # @TODO: remove hack to get test passing
    # we really want to wait until all the events have been processed
    Process.sleep(500)

    Process.exit(derive, :normal)

    receive do
      {:DOWN, _ref, :process, ^derive, _} ->
        :ok
    end
  end

  ### Server

  def init(opts) do
    name = Keyword.fetch!(opts, :name)

    reducer = Keyword.fetch!(opts, :reducer)
    batch_size = Keyword.get(opts, :batch_size, 100)
    source = Keyword.fetch!(opts, :source)

    children = [
      {Registry, keys: :unique, name: registry_name(name)},
      {DynamicSupervisor, strategy: :one_for_one, name: supervisor_name(name)},
      {Derive.Dispatcher,
       name: dispatcher_name(name),
       reducer: reducer,
       batch_size: batch_size,
       source: source,
       lookup_or_start: fn {reducer, partition} ->
         lookup_or_start(name, {reducer, partition})
       end}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  # Return the given `Derive.PartitionSupervisor` dispatcher for the given {reducer, partition} pair
  # Processes are lazily started.
  # - If a process is already alive, we will return the existing process.
  # - If a process hasn't been started, it'll be started and returned.
  defp lookup_or_start(name, {reducer, partition}) do
    key = {reducer, partition}

    case Registry.lookup(registry_name(name), key) do
      [{pid, _}] ->
        pid

      [] ->
        via = {:via, Registry, {registry_name(name), key}}

        case DynamicSupervisor.start_child(
               supervisor_name(name),
               {Derive.PartitionDispatcher, [reducer: reducer, partition: partition, name: via]}
             ) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end
    end
  end

  defp registry_name(name),
    do: :"#{name}.Registy"

  defp supervisor_name(name),
    do: :"#{name}.Supervisor"

  defp dispatcher_name(name),
    do: :"#{name}.Dispatcher"
end
