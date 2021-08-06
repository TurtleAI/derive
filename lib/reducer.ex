defmodule Derive.Reducer do
  use GenServer

  @type failure_mode() :: :skip | :halt

  @type partition() :: binary() | {module(), binary()}
  @type operation() :: any()
  @type event() :: any()

  @callback partition(event()) :: partition() | nil
  @callback handle(event()) :: operation()

  defmacro __using__(_options) do
    quote do
      @behaviour Derive.Reducer

      import Derive.Reducer.Change
    end
  end

end
