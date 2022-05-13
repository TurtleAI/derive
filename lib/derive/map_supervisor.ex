defmodule Derive.MapSupervisor do
  @moduledoc """
  A variant of `DynamicSupervisor` to start or lookup processes that
  are identified by a key.
  """

  use DynamicSupervisor

  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, :ok, opts)
  end

  @doc """
  The child_spec for a Registry to register child processes for this supervisor
  """
  def registry_child_spec(name),
    do: {Registry, keys: :unique, name: name}

  @doc """
  Start a child process or return an existing one by the given key
  - If the process is already exists for the key, it is returned
  - If the process doesn't exist for the key, it will be created with config in {mod, opts}
  """
  @spec start_child(
          Supervisor.supervisor(),
          Registry.registry(),
          any(),
          {module(), keyword()}
        ) :: pid()
  def start_child(supervisor, registry, key, {mod, opts}) do
    case Registry.lookup(registry, key) do
      [{pid, _}] ->
        pid

      [] ->
        via = {:via, Registry, {registry, key}}
        opts = Keyword.put(opts, :name, via)

        case DynamicSupervisor.start_child(supervisor, {mod, opts}) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end
    end
  end

  @impl true
  def init(:ok) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
