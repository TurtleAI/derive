defmodule Derive.Reducer do
  @type partition() :: binary() | {module(), binary()}

  @moduledoc """
  Specifies how some state can be kept up to date based on
  an event source c:source/1
  """

  @typedoc """
  A generic struct that represents an event.
  """
  @type event() :: any()

  @type operation() :: any()

  @doc """
  The process where events come for processing
  """
  @callback source() :: pid()

  @doc """
  For a given event, return a value by which to serialize the event processing.
  This lets the reducer process events with as much concurrency as possible.
  """
  @callback partition(event()) :: partition() | nil

  @doc """
  For a given event, return a operation that should be run as a result.
  This is usually for keeping state up to date.

  How the operation is processed depends on the sink.
  """
  @callback handle_event(event()) :: operation()

  @doc """
  Execute the operations that come from handle_event.
  These events will be processed in batches.
  """
  @callback commit_operations([operation()]) :: :ok

  defmacro __using__(_options) do
    quote do
      @behaviour Derive.Reducer
    end
  end
end
