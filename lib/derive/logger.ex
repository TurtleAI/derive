defmodule Derive.Logger do
  @moduledoc """
  A logger is a generic process or function that receives
  log messages for a Derive process and handle them appropriately.

  There are specific implementations such as:
  - The `Derive.Logger.RebuildProgressLogger` is a logger that shows a progress bar when rebuild a reducer
  - The `Derive.Logger.IOLogger` prints out log messages in dev mode

  The application interacts with the loggers through this module.
  """

  ### Client

  @type server :: pid() | atom() | function() | nil
  @type t :: server | [server()] | nil

  @doc """
  Log a multi that has been committed
  """
  def multi(server, multi),
    do: log(server, {:multi, multi})

  @spec append_logger(t(), server()) :: t()
  def append_logger(loggers, l),
    do: List.wrap(loggers) ++ [l]

  def error(logger, error) do
    log(logger, {:error, error})
    flush(logger)
  end

  @spec log(t(), term()) :: :ok
  def log(nil, _),
    do: :ok

  def log([], _),
    do: :ok

  def log(server, message) when is_pid(server) or is_atom(server) do
    GenServer.cast(server, {:log, message})
  end

  def log(func, message) when is_function(func, 1) do
    func.(message)
  end

  def log([server | rest], message) do
    log(server, message)
    log(rest, message)
  end

  def flush([logger | rest]) do
    flush(logger)
    flush(rest)
  end

  def flush(nil), do: :ok

  def flush(server) when is_pid(server) or is_atom(server),
    do: GenServer.call(server, :flush)

  def flush(_), do: :ok
end
