defmodule Derive.Dispatcher do
  use GenServer

  alias Derive.Util.MapOfSets

  @moduledoc """
  Responsible for keeping derived state up to date based on implementation `Derive.Reducer`

  Events are processed async but in order based on `Derive.Reducer.partition`.
  """
  def start_link(mod, opts) do
    {_dispatcher_opts, genserver_opts} =
      opts
      |> Keyword.split([])

    GenServer.start_link(__MODULE__, %{mod: mod}, genserver_opts)
  end

  @doc """
  Wait for all of the events to be processed.
  Must be called after an event has been persisted to a source.
  """
  def await_processed(dispatcher, events) when is_list(events) do
    for e <- events do
      await_processed(dispatcher, e)
    end
  end

  def await_processed(dispatcher, event) do
    GenServer.call(dispatcher, {:await_processed, event})
  end

  @impl true
  def init(%{mod: mod}) do
    Derive.Source.subscribe(mod.source(), self())
    {:ok, %{mod: mod, unprocessed_events: [], processed_awaiters: %{}}}
  end

  @impl true
  def handle_call({:await_processed, _}, _sender, %{unprocessed_events: []} = state) do
    {:reply, :ok, state}
  end

  def handle_call(
        {:await_processed, event},
        caller,
        %{unprocessed_events: unprocessed_events, processed_awaiters: processed_awaiters} = state
      ) do
    case Enum.member?(unprocessed_events, event) do
      false ->
        {:reply, :ok, state}

      true ->
        {:noreply,
         %{state | processed_awaiters: MapOfSets.put(processed_awaiters, event, caller)}}
    end
  end

  @impl true
  def handle_cast({:new_events, new_events}, %{unprocessed_events: unprocessed_events} = state) do
    GenServer.cast(self(), :process_events)
    {:noreply, %{state | unprocessed_events: unprocessed_events ++ new_events}}
  end

  def handle_cast(
        :process_events,
        %{
          mod: mod,
          unprocessed_events: unprocessed_events,
          processed_awaiters: processed_awaiters
        } = state
      ) do
    operations = Enum.map(unprocessed_events, &mod.handle_event/1)
    mod.commit_operations(operations)

    for e <- unprocessed_events do
      case processed_awaiters do
        %{^e => callers} ->
          for c <- callers do
            GenServer.reply(c, :ok)
          end

        %{} ->
          :ok
      end
    end

    new_processed_awaiters =
      Enum.reduce(unprocessed_events, processed_awaiters, fn e, acc ->
        Map.delete(acc, e)
      end)

    {:noreply,
     %{
       state
       | unprocessed_events: [],
         processed_awaiters: new_processed_awaiters
     }}
  end
end
