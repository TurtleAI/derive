defmodule Derive.Logger.RebuildProgressLogger do
  @moduledoc """
  A server for listening to rebuild progress events
  and printing a progress bar and other status.
  """

  use GenServer

  alias Derive.Timespan

  require Logger

  @type server :: pid() | atom()
  @type option :: {:replace, boolean()} | GenServer.option()

  @type t :: %__MODULE__{
          processed: non_neg_integer(),
          total: non_neg_integer(),
          timespan: Timespan.t(),
          replace: boolean(),
          last_logged: Timespan.timestamp()
        }
  defstruct processed: 0, total: 0, timespan: nil, last_logged: nil, replace: true

  alias Derive.Logger.RebuildProgressLogger, as: S
  alias Derive.Logger.Util.ProgressBar
  alias Derive.Options

  @bar_width 25
  @update_interval 1_000_000

  @spec start_link([option()]) :: {:ok, server()} | {:error, term()}
  def start_link(opts) do
    {replace, opts} = Keyword.pop(opts, :replace, true)
    GenServer.start_link(__MODULE__, %S{replace: replace}, opts)
  end

  ### Server

  @impl true
  def init(state),
    do: {:ok, state}

  @impl true
  def handle_cast(
        {:log, {:rebuild_started, %Options{reducer: reducer}, total}},
        state
      ) do
    IO.puts("REBUILD " <> Derive.Formatter.mod_to_string(reducer))

    %ProgressBar{value: 0, total: total, width: @bar_width, status: "starting... #{total} events"}
    |> ProgressBar.render()
    |> IO.write()

    now = :erlang.timestamp()

    {:noreply, %S{state | total: total, timespan: Timespan.start(now), last_logged: now}}
  end

  def handle_cast(
        {:log, {:events_processed, count}},
        %S{
          processed: processed,
          replace: replace
        } = state
      ) do
    now = :erlang.timestamp()
    new_processed = processed + count

    {new_state, progress} = next_progress(state, now, new_processed)

    if progress != nil do
      progress
      |> ProgressBar.render(replace: replace)
      |> IO.write()
    end

    {:noreply, new_state}
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
      width: @bar_width,
      status: "processed #{processed} events in #{elapsed_status}"
    }
    |> ProgressBar.render(replace: true)
    |> IO.write()

    {:noreply, %S{state | timespan: Timespan.stop(timespan)}}
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

  defp next_progress(
         %S{timespan: timespan, total: total, last_logged: last_logged, replace: replace} = state,
         now,
         new_processed
       ) do
    elapsed_timespan = Timespan.stop(timespan, now)

    elapsed_status =
      elapsed_timespan
      |> Timespan.elapsed()
      |> Timespan.format_elapsed()

    estimated_status =
      elapsed_timespan
      |> Timespan.estimate({new_processed, total})
      |> Timespan.format_elapsed()

    elapsed_microseconds = :timer.now_diff(now, last_logged)

    {progress_bar, new_last_logged} =
      if replace || elapsed_microseconds >= @update_interval do
        {%ProgressBar{
           value: new_processed,
           total: total,
           width: @bar_width,
           status:
             " " <>
               String.pad_trailing("#{new_processed}/#{total}", 20) <>
               "ELAPSED: #{elapsed_status} REMAINING: ~#{estimated_status}"
         }, now}
      else
        {nil, last_logged}
      end

    {%S{
       state
       | timespan: elapsed_timespan,
         processed: new_processed,
         last_logged: new_last_logged
     }, progress_bar}
  end
end
