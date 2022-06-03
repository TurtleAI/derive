defmodule Derive.PartitionSupervisor do
  @moduledoc """
  A supervisor to, well, supervise `Derive.PartitionDispatcher` processes.
  """

  alias Derive.{Reducer, Partition}

  @typedoc """
  A function to lookup or start a function given a reducer and partition
  """
  @type start_child :: ({Reducer.t(), Partition.id()} -> server())

  @type server :: pid() | atom()

  def start_link(opts \\ []),
    do: Supervisor.start_link(Derive.MapSupervisor, opts)

  def child_spec(opts),
    do: Derive.MapSupervisor.child_spec(opts)

  @doc """
  Return the given `Derive.PartitionDispatcher` dispatcher for the given {reducer, partition} pair
  Processes are lazily started.
  - If a process is already alive, we will return the existing process.
  - If a process hasn't been started, it'll be started and returned.
  """
  @spec start_child(server(), {Derive.Options.t(), Partition.id()}) :: pid()
  def start_child(server, {options, partition}) do
    Derive.MapSupervisor.start_child(
      server,
      {options.reducer, partition},
      {Derive.PartitionDispatcher, [options: options, partition: partition]}
    )
  end
end
