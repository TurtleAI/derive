defmodule Derive.Replies do
  @moduledoc """
  The result of waiting on events to be processed when calling `Derive.await/3`
  If there is a failure in any event, the status will be treated as an error.
  """

  defstruct [:status, :map]

  @type t :: %__MODULE__{
          status: status(),
          map: %{reply_key() => reply_value()}
        }

  @type status :: :ok | :error

  @type reply_key :: {any(), Derive.EventLog.event()}
  @type reply_value :: {:ok, any()} | {:error, any()} | :timeout

  def new(map) do
    %__MODULE__{status: get_status(map), map: map}
  end

  defp get_status(map) do
    case Enum.any?(map, fn
           {_key, {:error, _}} -> true
           _ -> false
         end) do
      true -> :error
      false -> :ok
    end
  end

  @doc """
  Get the reply for a Derive process processing a particular event.
  """
  @spec get(t(), reply_key()) :: reply_value()
  def get(%__MODULE__{map: map}, key) do
    Map.get(map, key)
  end
end
