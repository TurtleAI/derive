defmodule Derive do
  use Application

  def start(_type, _args) do
    children = [
      Derive.Repo
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Derive.Supervisor)
  end
end
