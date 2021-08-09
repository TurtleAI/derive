defmodule Derive.Reducer do
  @type failure_mode() :: :skip | :halt

  @type partition() :: binary() | {module(), binary()}

  @type operation() :: any()
  @type event() :: any()

  @doc """
  For a given event, return a value by which to serialize the event processing.
  This is useful to ensure events get processed in order.
  It's usually recommended to return a value as granular as possible to max out the concurrency.
  """
  @callback partition(event()) :: partition() | nil
  @callback handle(event()) :: operation()

  defmacro __using__(_options) do
    quote do
      @behaviour Derive.Reducer

      import Derive.Reducer.Change
    end
  end
end
