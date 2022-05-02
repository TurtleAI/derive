defmodule Derive.Logger.RebuildProgress do
  @moduledoc """
  A server for listening to rebuild progress events
  and printing a progress bar and other status.
  """

  use GenServer

  defstruct [:processed, :total]

  alias Derive.Logger.RebuildProgress, as: S
  alias Derive.Logger.Util.ProgressBar

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, :ok, opts)

  ### Server

  @impl true
  def init(state),
    do: {:ok, state}

  @impl true
  def handle_cast(
        {:log, {:rebuild_started, total}},
        state
      ) do
    %ProgressBar{value: 0, total: total, width: 50, status: "starting"}
    |> ProgressBar.render()
    |> IO.write()

    {:noreply, state}
  end

  @impl true
  def handle_cast(
        {:log, {:events_processed, count}},
        %S{processed: processed, total: total} = state
      ) do
    new_processed = processed + count

    %ProgressBar{
      value: new_processed,
      total: total,
      width: 50,
      status: " #{new_processed}/#{total}"
    }
    |> ProgressBar.render()
    |> IO.write()

    {:noreply, %{state | processed: new_processed}}
  end

  @impl true
  def handle_cast(
        {:log, :caught_up},
        %S{total: total} = state
      ) do
    %ProgressBar{value: total, total: total, width: 50, status: "done!"}
    |> ProgressBar.render()
    |> IO.write()

    {:noreply, state}
  end

  @impl true
  def handle_cast({:log, _}, state),
    do: {:noreply, state}
end
