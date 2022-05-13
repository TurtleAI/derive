defmodule Derive.MapSupervisor do
  @moduledoc """
  A variant of `DynamicSupervisor` to start or lookup processes that
  are identified by a key.
  """

  use Supervisor

  @type option :: Supervisor.option()
  @type supervisor :: Supervisor.supervisor()

  @typedoc """
  The key
  """
  @type key :: any()

  @spec start_link([option]) :: {:ok, supervisor()}
  def start_link(opts \\ []) do
    unless opts[:name], do: raise(ArgumentError, "expected :name option")

    Supervisor.start_link(__MODULE__, opts)
  end

  @doc """
  Start a child process or return an existing one by the given key
  - If the process is already exists for the key, it is returned
  - If the process doesn't exist for the key, it will be created with config in {mod, opts}
  """
  @spec start_child(
          Supervisor.supervisor(),
          term(),
          {module(), keyword()}
        ) :: pid()
  def start_child(supervisor, key, {mod, opts}) do
    registry = registry_name(supervisor)
    dynamic_supervisor = dynamic_supervisor_name(supervisor)

    case Registry.lookup(registry, key) do
      [{pid, _}] ->
        pid

      [] ->
        via = {:via, Registry, {registry, key}}
        opts = Keyword.put(opts, :name, via)

        case DynamicSupervisor.start_child(dynamic_supervisor, {mod, opts}) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end
    end
  end

  def init(opts) do
    name = Keyword.fetch!(opts, :name)

    children = [
      {Registry, keys: :unique, name: registry_name(name)},
      {DynamicSupervisor, strategy: :one_for_one, name: dynamic_supervisor_name(name)}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp registry_name(name),
    do: :"#{name}.Registy"

  defp dynamic_supervisor_name(name),
    do: :"#{name}.DynamicSupervisor"
end
