defmodule Derive.Logger.RebuildProgress do
  @moduledoc """
  A server for listening to rebuild progress events
  and printing a progress bar and other status.
  """

  use GenServer

  alias Derive.Timespan

  @type t :: %__MODULE__{
          processed: non_neg_integer(),
          total: non_neg_integer(),
          timespan: Timespan.t()
        }
  defstruct processed: 0, total: 0, timespan: nil

  alias Derive.Logger.RebuildProgress, as: S
  alias Derive.Logger.Util.ProgressBar

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, %S{}, opts)

  ### Server

  @impl true
  def init(state),
    do: {:ok, state}

  @impl true
  def handle_cast(
        {:log, {:rebuild_started, total}},
        state
      ) do
    %ProgressBar{value: 0, total: total, width: 50, status: "starting... #{total} events"}
    |> ProgressBar.render()
    |> IO.write()

    {:noreply, %{state | total: total, timespan: Timespan.start()}}
  end

  def handle_cast(
        {:log, {:events_processed, count}},
        %S{processed: processed, total: total, timespan: timespan} = state
      ) do
    new_processed = processed + count

    elapsed_timespan = Timespan.stop(timespan)

    elapsed_status =
      elapsed_timespan
      |> Timespan.elapsed()
      |> Timespan.format_elapsed()

    estimated_status =
      elapsed_timespan
      |> Timespan.estimate({new_processed, total})
      |> Timespan.format_elapsed()

    %ProgressBar{
      value: new_processed,
      total: total,
      width: 50,
      status:
        " #{new_processed}/#{total} ELAPSED: #{elapsed_status} ESTIMATE: #{estimated_status}"
    }
    |> ProgressBar.render(replace: true)
    |> IO.write()

    {:noreply, %{state | processed: new_processed}}
  end

  def handle_cast(
        {:log, :caught_up},
        %S{processed: processed, total: total, timespan: timespan} = state
      ) do
    elapsed_status =
      Timespan.stop(timespan)
      |> Timespan.elapsed()
      |> Timespan.format_elapsed()

    %ProgressBar{
      value: total,
      total: total,
      width: 50,
      status: "processed #{processed} events in #{elapsed_status}"
    }
    |> ProgressBar.render(replace: true)
    |> IO.write()

    {:noreply, %{state | timespan: Timespan.stop(timespan)}}
  end

  def handle_cast({:log, _msg}, state),
    do: {:noreply, state}
end
