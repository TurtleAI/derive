defmodule Derive.Reducer.Error do
  defstruct [:events, :failed_events]

  def events_without_failed_event(%Derive.Reducer.Error{}) do
    []
  end
end
