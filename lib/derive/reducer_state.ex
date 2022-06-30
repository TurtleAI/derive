defmodule Derive.ReducerState do
  @moduledoc """
  Behavior for stateful reducers that can have their state rebuilt from scratch
  """

  @doc """
  Reset the state so we can start processing from the first event
  This operation should reset the state in *all* partitions
  """
  @callback reset_state() :: :ok

  @doc """
  Whether the state of the reducer needs to be rebuilt.
  This can happen if the state is invalidated manually or if the version is updated.
  """
  @callback needs_rebuild?() :: boolean()

  defmacro __using__(_options) do
    quote do
      @behaviour Derive.ReducerState
    end
  end

  @doc """
  Whether a module implements the `Derive.ReducerState` behavior
  """
  @spec implemented?(atom()) :: boolean()
  def implemented?(module) do
    behaviours = Keyword.get(module.__info__(:attributes), :behaviour, [])
    Enum.member?(behaviours, __MODULE__)
  end
end
