defmodule Derive.Logger.DevLogger do
  @moduledoc """
  A function-based logger for use in development mode.
  """

  require Logger

  def log({:error, message}) do
    Logger.error(inspect(message))
  end

  def log({:info, message}) do
    Logger.info(inspect(message))
  end

  def log({:warn, message}) do
    Logger.warn(inspect(message))
  end

  def log(message) do
    Logger.info(inspect(message))
  end
end
