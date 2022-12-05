defmodule Derive.Timespan do
  @moduledoc """
  A tuple of start/stop times to measure how long operations take

  The first value is the start time and the second value is the end time.
  If a timespan hasn't been stopped yet, it will
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

  @doc """
  Start a new timespan with just a start time.
  If you don't pass in the start time, it will default to `:erlang.timestamp()`
  """
  @spec start(timestamp()) :: {timestamp(), nil}
  def start(timestamp),
    do: {timestamp, nil}

  @doc """
  Start a timespan using the current system time
  """
  @spec start() :: {timestamp(), nil}
  def start,
    do: start(:erlang.timestamp())

  @doc """
  Complete a timespan at an end time

  If you don't pass in the end time, it will default to `:erlang.timestamp()`
  """
  @spec stop({timestamp(), nil}, timestamp()) :: t()
  def stop({tstart, _tend}, timestamp),
    do: {tstart, timestamp || :erlang.timestamp()}

  @doc """
  Complete a timestamp using the current system timestamp
  """
  @spec stop({timestamp(), nil}) :: t()
  def stop({tstart, tend}),
    do: stop({tstart, tend}, :erlang.timestamp())

  @doc """
  The amount of time in microseconds that has elapsed for a timespan
  """
  @spec elapsed(t()) :: microseconds()
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

    separate_units(seconds, @units)
    |> Enum.drop_while(fn {_unit, v} -> v == 0 end)
    |> Enum.map_join(" ", fn {unit, value} -> "#{value}#{unit}" end)
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
