defmodule Derive.Logger.RebuildProgressLogger do
  @moduledoc """
  A server for listening to rebuild progress events
  and printing a progress bar and other status.
  """

  use GenServer

  alias Derive.Timespan

  require Logger

  @type t :: %__MODULE__{
          processed: non_neg_integer(),
          total: non_neg_integer(),
          timespan: Timespan.t()
        }
  defstruct processed: 0, total: 0, timespan: nil

  alias Derive.Logger.RebuildProgressLogger, as: S
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

  def handle_cast({:log, message}, state) do
    handle_log(message)
    {:noreply, state}
  end

  defp handle_log({:error, {:multi_op, multi}}) do
    Logger.error(multi)
  end

  defp handle_log(_) do
    :ok
  end
end
