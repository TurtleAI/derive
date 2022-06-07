defmodule Derive.Logger.IOLogger do
  @moduledoc """
  A function-based logger for use in development and production mode.
  """

  require Logger

  alias Derive.Partition

  import Derive.Formatter, only: [mod_to_string: 1]

  def log({:error, error, stacktrace}),
    do: Logger.error(Exception.format(:error, error, stacktrace))

  def log({:info, message}),
    do: Logger.info(inspect(message))

  def log({:warn, message}),
    do: Logger.warn(inspect(message))

  def log({:caught_up, reducer, %Partition{cursor: cursor}}) do
    Logger.info("#{mod_to_string(reducer)}: ALL CAUGHT UP TO #{cursor}")
  end

  def log(_message),
    do: :ok
end
