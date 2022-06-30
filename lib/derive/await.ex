defmodule Derive.Await do
  @moduledoc """
  The result of waiting on events to be processed when calling `Derive.await/3`
  If there is a failure in any event, the status will be treated as an error.
  """

  defstruct [:status, :replies]

  @type t :: %__MODULE__{
          status: status(),
          replies: %{reply_key() => reply_value()}
        }

  @type status :: :ok | :error

  @type reply_key :: any()
  @type reply_value :: {:ok, any()} | {:error, any()}

  def get(%__MODULE__{replies: replies}, key) do
    Map.get(replies, key)
  end
end
