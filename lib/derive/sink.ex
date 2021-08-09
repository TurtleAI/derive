defmodule Derive.Sink do
  @moduledoc """
  A process that persists state changes.

  A state change can be any arbitrary value. For example, see `Derive.Reducer.Change.Put`.

  There are some built-in sinks such as `Derive.Sink.InMemory` or users can implement their own.
  """

  def handle_changes(sink, changes),
    do: GenServer.call(sink, {:handle_changes, changes})
end
