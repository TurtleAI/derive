defmodule Derive do
  use Application

  def start(_type, _args) do
    children = [
      Derive.Repo,
      {Registry, keys: :unique, name: Derive.Registry},
      Derive.PartitionSupervisor
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Derive.Supervisor)
  end
end
