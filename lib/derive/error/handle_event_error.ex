defmodule Derive.Error.HandleEventError do
  @moduledoc """
  Error when trying to commit some operations
  """

  defexception [:operation, :stacktrace]

  @type t :: %__MODULE__{
          operation: Derive.State.EventOp.t(),
          stacktrace: Exception.stacktrace() | nil
        }

  def message(%__MODULE__{operation: operation}) do
    "handle_event failed #{inspect(operation)}"
  end
end
