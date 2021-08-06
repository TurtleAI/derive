defmodule Derive.Sink do
  def handle_changes(sink, changes),
    do: GenServer.call(sink, {:handle_changes, changes})
end
