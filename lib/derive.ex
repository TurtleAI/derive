defmodule Derive do
  @moduledoc ~S"""
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

  @type option :: Derive.Options.option()
  @type server :: Supervisor.supervisor()

  use Supervisor

  alias Derive.{Dispatcher, Dispatcher, EventLog, Options}

  @spec start_link([option()]) :: {:ok, server()} | {:error, term()}
  def start_link(opts \\ []) do
    unless opts[:reducer], do: raise(ArgumentError, "expected :reducer option")

    reducer = Keyword.fetch!(opts, :reducer)
    name = Keyword.fetch!(opts, :name)
    source = Keyword.fetch!(opts, :source)
    mode = Keyword.get(opts, :mode, :catchup)

    {source_spec, source_server} = spec_and_server(name, :source, source)

    derive_opts = %Derive.Options{
      reducer: Keyword.fetch!(opts, :reducer),
      name: name,
      mode: Keyword.get(opts, :mode, :catchup),
      batch_size: Keyword.get(opts, :batch_size, 100),
      source: source_server,
      logger: Keyword.get(opts, :logger)
    }

    # Some reducers have an optional setup step
    derive_opts.reducer.setup(derive_opts)

    # In dev and prod mode, by default, we'd like to validate that the reducer
    # version matches what is currently in the database to avoid subtle errors.
    #
    # For example, part of the state could have been built with an older reducer version
    # and the rest of the state can be built with a newer version.
    #
    # In test mode, we disable this by default to make testing easier
    validate_version = Keyword.get(opts, :validate_version, Mix.env() != :test)

    cond do
      validate_version && needs_rebuild?(derive_opts.reducer) && mode != :rebuild ->
        {:error, {:needs_rebuild, reducer}}

      true ->
        supervisor_opts = Keyword.take(opts, [:name])
        Supervisor.start_link(__MODULE__, {derive_opts, source_spec}, supervisor_opts)
    end
  end

  defp needs_rebuild?(reducer) do
    function_exported?(reducer, :needs_rebuild?, 0) && reducer.needs_rebuild?()
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

  It is also possible to pass call `Derive.await(server, :catchup)` to wait until a server is fully caught up.

  Events are not considered processed until *all* operations produced by `Derive.Reducer.handle_event/1`
  have been committed by `Derive.Reducer.commit/1`
  """
  @spec await(server(), [EventLog.event()] | :catchup) :: :ok
  def await(server, :catchup) do
    GenServer.call(child_process(server, :dispatcher), {:await_catchup}, 30_000)
    :ok
  end

  def await(server, events) do
    dispatcher = child_process(server, :dispatcher)
    partition_supervisor = child_process(server, :supervisor)

    options = %Options{reducer: reducer} = Derive.Dispatcher.get_options(dispatcher)

    servers_with_messages =
      for event <- events, reducer.partition(event) != nil do
        partition = reducer.partition(event)

        partition_dispatcher =
          Derive.PartitionSupervisor.start_child(partition_supervisor, {options, partition})

        {partition_dispatcher, {:await, event}}
      end

    Derive.Ext.GenServer.call_many(servers_with_messages, 30_000)

    :ok
  end

  @doc """
  Wait for all the events to be processed by all Derive processes
  """
  @spec await_many([server()], [EventLog.event()]) :: :ok
  def await_many(servers, events) do
    Enum.each(servers, fn s ->
      await(s, events)
    end)
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

    dispatcher = Process.whereis(child_process(name, :dispatcher))
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
  @spec alive?(server()) :: boolean()
  def alive?(server) when is_pid(server),
    do: Process.alive?(server)

  def alive?(server),
    do: Process.whereis(server) != nil

  ### Server

  def init({%Options{name: name} = derive_opts, child_specs}) do
    dispatcher_opts = [
      name: child_process(name, :dispatcher),
      options: derive_opts,
      partition_supervisor: child_process(name, :supervisor)
    ]

    children =
      child_specs ++
        derive_opts.reducer.child_specs(derive_opts) ++
        [
          {Dispatcher, dispatcher_opts},
          {Derive.MapSupervisor, name: child_process(name, :supervisor)}
        ]

    # :rest_for_one because if the dispatcher dies,
    # we want all of the `Derive.PartitionDispatcher` to die
    Supervisor.init(children, strategy: :rest_for_one)
  end

  # In some cases, we want the Derive process to spawn and supervise an
  # event log rather than handling it externally
  defp spec_and_server(derive_name, child_id, {mod, opts}) do
    default_child_process_name = child_process(derive_name, child_id)
    opts = Keyword.put_new(opts, :name, default_child_process_name)

    child_spec = %{
      id: child_id,
      start: {mod, :start_link, [opts]}
    }

    {[child_spec], Keyword.fetch!(opts, :name)}
  end

  defp spec_and_server(_id, _default_name, source),
    do: {[], source}

  defp child_process(derive_name, child_id) do
    :"#{derive_name}.#{child_id}"
  end
end
