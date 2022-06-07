defmodule Derive.Error.CommitError do
  @moduledoc """
  Error when trying to commit some operations
  """

  defexception [:cursor, :error, :stacktrace, :operation]

  @type t :: %__MODULE__{
          cursor: Derive.EventLog.cursor() | nil,
          stacktrace: System.stacktrace() | nil,
          operation: Derive.State.EventOp.t()
        }

  def message(%__MODULE__{cursor: cursor, error: error}) do
    "commit failed [#{cursor}] #{inspect(error)}"
  end
end
