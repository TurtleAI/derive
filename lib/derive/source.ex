defmodule Derive.Source do
  def subscribe(source, subscriber),
    do: GenServer.call(source, {:subscribe, subscriber})
end
