defmodule Derive.Logger do
  @moduledoc """
  A logger is a generic process or function that can receive
  log messages related to a Derive process
  """

  ### Client

  @type server :: pid() | atom() | function() | nil
  @type t :: server | [server()] | nil

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

  def log([], _),
    do: :ok

  def log(server, message) when is_pid(server) or is_atom(server),
    do: GenServer.cast(server, {:log, message})

  def log(func, message) when is_function(func, 1) do
    func.(message)
  end

  def log([server | rest]) do
    log(server)
    log(rest)
  end
end
