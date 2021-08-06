defmodule Derive.Dispatcher do
  use GenServer

  def start_link(mod, opts) do
    {_dispatcher_opts, genserver_opts} =
      opts
      |> Keyword.split([])

    GenServer.start_link(__MODULE__, %{mod: mod}, genserver_opts)
  end

  def init(%{mod: mod}) do
    Derive.Source.subscribe(mod.source(), self())
    {:ok, %{mod: mod}}
  end

  def wait_for_catchup(dispatcher) do
    Process.sleep(200)
    :ok
  end

  def handle_cast({:new_events, new_events}, %{mod: mod} = state) do
    changes = Enum.map(new_events, &mod.handle/1)
    Derive.Sink.handle_changes(mod.sink(), changes)
    {:noreply, state}
  end
end
