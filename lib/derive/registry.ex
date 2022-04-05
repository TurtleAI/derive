defmodule Derive.Registry do
  @moduledoc """
  Registry to keep track of dispatchers for specific partitions
  """

  def via(key),
    do: {:via, Registry, {__MODULE__, key}}
end
