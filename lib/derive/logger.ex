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

  @spec append_logger(t(), server()) :: t()
  def append_logger(loggers, l),
    do: List.wrap(loggers) ++ [l]

  @spec log(t(), any()) :: :ok
  def log(nil, _),
    do: :ok

  def log([], _),
    do: :ok

  def log(server, message) when is_pid(server) or is_atom(server),
    do: GenServer.cast(server, {:log, message})

  def log(func, message) when is_function(func, 1) do
    func.(message)
  end

  def log([server | rest], message) do
    log(server, message)
    log(rest, message)
  end
end
