defmodule Derive.Dispatcher do
  use GenServer

  @moduledoc """
  Responsible for keeping a derived view up to date based on the configuration as specified by a `Derive.Reducer`

  Events are processed async but in order based on `Derive.Reducer.partition`.
  """

  def start_link(mod, opts) do
    {_dispatcher_opts, genserver_opts} =
      opts
      |> Keyword.split([])

    GenServer.start_link(__MODULE__, %{mod: mod}, genserver_opts)
  end

  def wait_for_catchup(dispatcher) do
    GenServer.call(dispatcher, :wait_for_catchup)
  end

  def init(%{mod: mod}) do
    Derive.Source.subscribe(mod.source(), self())
    {:ok, %{mod: mod, unprocessed_events: [], waiters: []}}
  end

  def handle_call(:wait_for_catchup, _sender, %{unprocessed_events: []} = state) do
    {:reply, :ok, state}
  end
  def handle_call(:wait_for_catchup, sender, %{waiters: waiters} = state) do
    {:noreply, %{state | waiters: [sender | waiters]}}
  end

  def handle_cast({:new_events, new_events}, %{unprocessed_events: unprocessed_events} = state) do
    GenServer.cast(self(), :process_events)
    {:noreply, %{state | unprocessed_events: unprocessed_events ++ new_events}}
  end

  def handle_cast(:process_events, %{mod: mod, unprocessed_events: unprocessed_events, waiters: waiters} = state) do
    changes = Enum.map(unprocessed_events, &mod.handle/1)
    Derive.Sink.handle_changes(mod.sink(), changes)

    for w <- waiters do
      GenServer.reply(w, :ok)
    end

    {:noreply, %{state | unprocessed_events: [], waiters: []}}
  end

end
