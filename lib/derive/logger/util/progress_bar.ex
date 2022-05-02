defmodule Derive.Logger.Util.ProgressBar do
  @moduledoc """
  Data structure that represents a progress bar
  and its status
  """

  defstruct [:value, :total, :status, width: 50]

  alias __MODULE__

  @filled "█"
  @empty "░"

  def render(%ProgressBar{value: value, total: total, status: status, width: width}, opts \\ []) do
    replace = Keyword.get(opts, :replace, false)

    filled_width =
      case total do
        0 -> 0
        total -> round(value / total * width)
      end

    unfilled_width = width - filled_width

    prefix =
      case replace do
        true -> IO.ANSI.clear_line() <> "\r"
        false -> ""
      end

    suffix =
      case status do
        nil -> ""
        status -> " " <> status
      end

    prefix <>
      String.duplicate(@filled, filled_width) <>
      String.duplicate(@empty, unfilled_width) <>
      suffix
  end
end
