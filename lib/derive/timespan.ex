defmodule Derive.Timespan do
  @moduledoc """
  A tuple of start/stop times to measure how long operations take
  """

  @typedoc """
  A tuple with the start and stop times
  """
  @type t() :: {timestamp(), timestamp()}

  @typedoc """
  An erlang timestamp.
  Number of microseconds since the epoch.
  """
  @type timestamp :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @type progress :: {non_neg_integer(), non_neg_integer()}

  @type microseconds :: non_neg_integer()

  @spec start() :: {timestamp(), nil}
  def start,
    do: {:erlang.timestamp(), nil}

  @spec stop({timestamp(), nil}) :: t()
  def stop({tstart, nil}),
    do: {tstart, :erlang.timestamp()}

  @doc """
  The amount of time in microseconds that has elapsed for a timespan
  """
  @spec elapsed(t()) :: integer()
  def elapsed({tstart, tend}),
    do: :timer.now_diff(tend, tstart)

  @doc """
  Whether two timespans overlap
  """
  @spec overlaps?(t(), t()) :: boolean()
  def overlaps?({tstart1, tend1}, {tstart2, tend2}) do
    cond do
      tend1 < tstart2 ->
        false

      tend2 < tstart1 ->
        false

      true ->
        true
    end
  end

  @units h: 3600, m: 60, s: 1

  @doc """
  Given the progress of an operation so far, meaning
  the number of records progress, total records processed,
  and the timespan elapsed so far, estimate amount of time
  remaining left to process everything
  """
  @spec estimate(t(), progress()) :: microseconds()
  def estimate(elapsed_timespan, {processed, total}) do
    time_elapsed = elapsed(elapsed_timespan)
    div((total - processed) * time_elapsed, processed)
  end

  @doc """
  Format microseconds as a human readable format
  """
  @spec format_elapsed(microseconds()) :: binary()
  def format_elapsed(elapsed_microseconds) do
    seconds = div(elapsed_microseconds, 1_000_000)
    [h: h, m: m, s: s] = separate_units(seconds, @units)
    "#{h}h #{m}m #{s}s"
  end

  defp separate_units(value, conversions) do
    {_, units} = do_separate_units(value, conversions)
    :lists.reverse(units)
  end

  defp do_separate_units(value, conversions) do
    Enum.reduce(conversions, {value, []}, fn {unit, divisor}, {n, acc} ->
      {rem(n, divisor), [{unit, div(n, divisor)} | acc]}
    end)
  end
end
