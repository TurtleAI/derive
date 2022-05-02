defmodule Derive.Logger do
  ### Client

  @type server :: pid() | atom() | nil

  @doc """
  Log a multi that has been committed
  """
  def committed(server, multi),
    do: log(server, {:committed, multi})

  @doc """
  Fetch all of the multis in the order they were committed
  """
  def fetch(server),
    do: GenServer.call(server, :fetch)

  @spec log(server(), any()) :: :ok
  def log(nil, _),
    do: :ok

  def log(server, message),
    do: GenServer.cast(server, {:log, message})
end
