defmodule Derive.Error.HandleEventError do
  @moduledoc """
  Error when trying to commit some operations
  """

  defexception [:operation]

  @type t :: %__MODULE__{
          operation: Derive.State.EventOp.t()
        }

  def message(%__MODULE__{operation: operation}) do
    "handle_event failed #{inspect(operation)}"
  end
end
