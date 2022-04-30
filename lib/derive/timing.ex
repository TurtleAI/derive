defmodule Derive.Timespan do
  @moduledoc """
  A tuple of start/stop times to measure how long operations take
  """

  @typedoc """
  A simple tuple with the start and stop times,
  each in the Erlang timestamp format
  """
  @type t() :: {timestamp(), timestamp()}

  @type timestamp :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @spec start() :: {timestamp(), nil}
  def start,
    do: {:erlang.timestamp(), nil}

  @spec stop({timestamp(), nil}) :: t()
  def stop({tstart, nil}),
    do: {tstart, :erlang.timestamp()}

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
end
