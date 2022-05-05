defmodule Derive do
  @moduledoc """
  Derive keeps a derived state in sync with an event log based on the behavior  in `Derive.Reducer`.

  Once the process has been started, it will automatically catch up to the latest version
  derived state.
  Then it will start listening to the event log for new events ensure the state stays up-to-date.

  The event log can be any ordered log of events, such as a Postgres table or an in memory process.
  It only needs to implement the `Derive.Eventlog` interface.

  Derived state is generic too.
  For example, it can be a set of Postgres tables or an in-memory GenServer.

  Events are processed as follows:
  - Events are processed from the configured source
  - They are processed by `c:Derive.Reducer.handle_event/1` and produce a `Derive.State.MultiOp`,
    a data structure to represent a state change.
  - This state change is applied using `c:Derive.Reducer.commit/1`.
    For Ecto, this is a database transaction.
    For an in-memory implementation, it's simply a state change.
  """

  @type option :: {:show_progress, boolean()} | Derive.Dispatcher.option()
  @type server :: Supervisor.supervisor()

  use Supervisor

  @spec start_link([option()]) :: {:ok, server()}
  def start_link(opts \\ []) do
    supervisor_opts = Keyword.take(opts, [:name])
    Supervisor.start_link(__MODULE__, opts, supervisor_opts)
  end

  ### Client
  @doc """
  Wait for all of the events to be processed by all of the matching partitions as defined by
  `Derive.Reducer.partition/1`

  If the event has already been processed, this will complete immediately
  If the event has not yet been processed, this will block until it completes processing

  Events are not considered processed until *all* operations produced by `Derive.Reducer.handle_event/1`
  have been committed by `Derive.Reducer.commit/1`
  """
  @spec await(server(), [Derive.EventLog.event()]) :: :ok
  def await(server, events),
    do: Derive.Dispatcher.await(dispatcher_name(server), events)

  @doc """
  Rebuilds the state of a reducer.
  This means the state will get reset to its initial state,
  Then all events from the event source will get reprocessed until the state is caught up again
  """
  @spec rebuild(Derive.Reducer.t(), [option()]) :: :ok
  def rebuild(reducer, opts \\ []) do
    name = reducer

    {:ok, rebuild_progress} =
      case Keyword.get(opts, :show_progress, false) do
        true -> Derive.Logger.RebuildProgress.start_link()
        false -> {:ok, nil}
      end

    derive_opts =
      Keyword.merge(opts,
        reducer: reducer,
        name: name,
        mode: :rebuild,
        logger: rebuild_progress
      )

    event_log = Keyword.fetch!(derive_opts, :source)

    count = Derive.EventLog.count(event_log)

    Derive.Logger.log(rebuild_progress, {:rebuild_started, count})

    {:ok, derive} = start_link(derive_opts)

    dispatcher = Process.whereis(dispatcher_name(name))
    Process.monitor(dispatcher)

    # In :rebuild mode, the dispatcher will shut down when
    # it has fully caught up
    # So we can block until it has completed, then kill the entire
    # process tree.
    receive do
      {:DOWN, _ref, :process, ^dispatcher, _} ->
        Process.exit(derive, :normal)
        :ok
    end
  end

  @doc """
  Gracefully shutdown a derive process and all of its children.
  Completes synchronously, so blocks until the process is shut down
  """
  @spec stop(server()) :: :ok
  def stop(server),
    do: Supervisor.stop(server)

  ### Server

  def init(opts) do
    # besides :name, all options are forwarded to the dispatcher
    {derive_opts, dispatcher_opts} = Keyword.split(opts, [:name])

    name = Keyword.fetch!(derive_opts, :name)
    source = Keyword.fetch!(dispatcher_opts, :source)

    {source_spec, source_server} = source_spec_and_server(source, source_name(name))

    dispatcher_opts =
      Keyword.merge(dispatcher_opts,
        name: dispatcher_name(name),
        source: source_server,
        lookup_or_start: fn {reducer, partition} ->
          lookup_or_start(name, {reducer, partition})
        end
      )

    children =
      source_spec ++
        [
          {Registry, keys: :unique, name: registry_name(name)},
          {Derive.Dispatcher, dispatcher_opts},
          {DynamicSupervisor, strategy: :one_for_one, name: supervisor_name(name)}
        ]

    # :rest_for_one because if the dispatcher dies,
    # we want all of the `Derive.PartitionDispatcher` to die
    Supervisor.init(children, strategy: :rest_for_one)
  end

  # In some cases, we want the Derive process to spawn and supervise an
  # event log rather than handling it externally
  defp source_spec_and_server({mod, opts}, default_name) do
    {[
       %{
         id: :source,
         start: {mod, :start_link, [opts]}
       }
     ], Keyword.get(opts, :name, default_name)}
  end

  defp source_spec_and_server(source, _default_name) do
    {[], source}
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

  # process of the source if this Derive process is responsible
  # for spwaning it
  defp source_name(name),
    do: :"#{name}.Source"

  # process of a child process given the Derive process name
  defp registry_name(name),
    do: :"#{name}.Registy"

  # process of the dynamic supervisor for Derive.PartitionDispatcher
  # given the Derive process name
  defp supervisor_name(name),
    do: :"#{name}.Supervisor"

  # process of the `Derive.Dispatcher`
  defp dispatcher_name(name),
    do: :"#{name}.Dispatcher"
end
