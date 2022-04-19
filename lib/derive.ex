defmodule Derive do
  use Application

  # TODO: refactor to reusable process https://keathley.io/blog/reusable-libraries.html

  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Derive.Registry},
      Derive.PartitionSupervisor
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Derive.Supervisor)
  end
end
