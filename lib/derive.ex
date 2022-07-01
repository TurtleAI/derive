defmodule Derive do
  @moduledoc ~S"""
  Derive provides the infrastructure to keep derived state in sync
  with an event log based on the behavior defined in `Derive.Reducer`.

  The event log and the state are both genreic.
  - A Postgres users table can be kept in sync with a Postgres events table
  - The state of an in-memory Agent can be kept in sync with a json text field

  The event log can be any process that implements the `Derive.Eventlog` interface.

  How a state is updated is defined by the implementation in `c:Derive.Reducer.process_events/2`

  The state is eventually consistent. Once a `Derive` process has started,
  it processes events in order until the state has been caught up.
  """

  @type option :: Derive.Options.option()
  @type server :: Supervisor.supervisor()

  use Supervisor

  alias Derive.{Dispatcher, Dispatcher, Replies, PartitionSupervisor, EventLog, Options}

  # The default timeout to use in waiting for things
  @default_await_timeout 30_000

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
  @spec await_catchup(server(), timeout()) :: :ok
  def await_catchup(server, timeout \\ @default_await_timeout) do
    GenServer.call(child_process(server, :dispatcher), :await_catchup, timeout)
  end

  @doc """
  Wait for all events to be processed by the Derive process.

  If all events are successfully processed `{:ok, %Replies{}}` will be returned.
  If there is an error on any event (even a partial failure), `{:error, %Replies{}}` will be returned.

  In the simplest use case, you can call `Derive.await(server, events)` and it will block until processing is done.

  If the an event has already been processed by the server, it will be considered processed with no delay.
  """
  @spec await(server(), [EventLog.event()], timeout()) ::
          {:ok, Replies.t()} | {:error, Replies.t()}
  def await(server, events, timeout \\ @default_await_timeout),
    do: await_many([server], events, timeout)

  @doc """
  Wait for all the events to be processed by all Derive processes
  """
  @spec await_many([server()], [EventLog.event()], timeout()) ::
          {:ok, Replies.t()} | {:error, Replies.t()}
  def await_many(servers, events, timeout \\ @default_await_timeout) do
    await_messages = for server <- servers, message <- await_messages(server, events), do: message

    replies = Derive.Ext.GenServer.call_many(await_messages, timeout)

    for {key, reply} <- replies, into: %{} do
      case reply do
        {:reply, :ok} -> {key, :ok}
        {:reply, {:error, error}} -> {key, {:error, error}}
        {:error, error} -> {key, {:error, error}}
      end
    end
    |> Replies.new()
    |> then(fn %Replies{status: status} = reply ->
      {status, reply}
    end)
  end

  defp await_messages(server, events) do
    partition_supervisor = child_process(server, :supervisor)
    options = %Options{reducer: reducer} = Agent.get(child_process(server, :options), & &1)

    for event <- events, partition = reducer.partition(event), partition != nil do
      dispatcher_process =
        PartitionSupervisor.start_child(partition_supervisor, {options, partition})

      {{server, event}, dispatcher_process, {:await, event}}
    end
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
          # We use an agent to store the options for this derive process
          # We want this to be a dedicated process so we aren't waiting
          # for a busy process to finish its work before returning the config
          %{
            id: :options,
            start:
              {Agent, :start_link, [fn -> derive_opts end, [name: child_process(name, :options)]]}
          },
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
