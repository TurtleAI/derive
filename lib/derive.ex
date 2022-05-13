defmodule Derive do
  @moduledoc """
  Derive keeps a derived state in sync with an event log based on the behavior in `Derive.Reducer`.

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

  alias Derive.{Dispatcher, Dispatcher, PartitionDispatcher, MapSupervisor, EventLog}

  @spec start_link([option()]) :: {:ok, server()}
  def start_link(opts \\ []) do
    supervisor_opts = Keyword.take(opts, [:name])
    Supervisor.start_link(__MODULE__, opts, supervisor_opts)
  end

  def child_spec(opts) do
    reducer = Keyword.fetch!(opts, :reducer)

    %{
      id: reducer,
      start: {__MODULE__, :start_link, [opts]},
      shutdown: 10_000,
      restart: :transient,
      type: :worker
    }
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
  @spec await(server(), [EventLog.event()]) :: :ok
  def await(server, events),
    do: Dispatcher.await(dispatcher_name(server), events)

  @doc """
  Wait for all the events to be processed by all Derive processes
  """
  @spec await_many([server()], [EventLog.event()]) :: :ok
  def await_many([], _events),
    do: :ok

  def await_many([server | rest], events) do
    await(server, events)
    await_many(rest, events)
  end

  @doc """
  Rebuilds the state of a reducer.
  This means the state will get reset to its initial state,
  Then all events from the event source will get reprocessed until the state is caught up again
  """
  @spec rebuild(Derive.Reducer.t(), [option()]) :: :ok
  def rebuild(reducer, opts \\ []) do
    name = reducer

    logger = Keyword.get(opts, :logger)

    {:ok, rebuild_progress} =
      case Keyword.get(opts, :show_progress, false) do
        true -> Derive.Logger.RebuildProgressLogger.start_link()
        false -> {:ok, nil}
      end

    derive_opts =
      Keyword.merge(opts,
        reducer: reducer,
        name: name,
        mode: :rebuild,
        logger: Derive.Logger.append_logger(logger, rebuild_progress)
      )

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
  @spec stop(server()) :: :ok | :already_stopped
  def stop(server) do
    case alive?(server) do
      true -> Supervisor.stop(server)
      false -> :already_stopped
    end
  end

  @doc """
  Whether the Derive instance is alive
  """
  def alive?(server) when is_pid(server),
    do: Process.alive?(server)

  def alive?(server),
    do: Process.whereis(server) != nil

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
          # Return the given `Derive.PartitionSupervisor` dispatcher for the given {reducer, partition} pair
          # Processes are lazily started.
          # - If a process is already alive, we will return the existing process.
          # - If a process hasn't been started, it'll be started and returned.
          MapSupervisor.start_child(
            supervisor_name(name),
            {reducer, partition},
            {PartitionDispatcher, [reducer: reducer, partition: partition]}
          )
        end
      )

    children =
      source_spec ++
        [
          {Dispatcher, dispatcher_opts},
          {MapSupervisor, name: supervisor_name(name)}
        ]

    # :rest_for_one because if the dispatcher dies,
    # we want all of the `Derive.PartitionDispatcher` to die
    Supervisor.init(children, strategy: :rest_for_one)
  end

  # In some cases, we want the Derive process to spawn and supervise an
  # event log rather than handling it externally
  defp source_spec_and_server({mod, opts}, default_name) do
    opts = Keyword.put_new(opts, :name, default_name)

    {[
       %{
         id: :source,
         start: {mod, :start_link, [opts]}
       }
     ], Keyword.fetch!(opts, :name)}
  end

  defp source_spec_and_server(source, _default_name) do
    {[], source}
  end

  # process of the source if this Derive process is responsible
  # for spwaning it
  defp source_name(name),
    do: :"#{name}.Source"

  # process of the dynamic supervisor for Derive.PartitionDispatcher
  # given the Derive process name
  defp supervisor_name(name),
    do: :"#{name}.Supervisor"

  # process of the `Derive.Dispatcher`
  defp dispatcher_name(name),
    do: :"#{name}.Dispatcher"
end
