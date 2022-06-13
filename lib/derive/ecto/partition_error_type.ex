defmodule Derive.Ecto.PartitionErrorType do
  @moduledoc """
  Used for serializing and de-serializing an error
  """

  use Ecto.Type

  alias Derive.PartitionError

  def type, do: :map

  def cast(%PartitionError{} = error), do: {:ok, error}
  def cast(_), do: :error

  def dump(%PartitionError{type: type, message: message, cursor: cursor, batch: batch}) do
    {:ok, %{"type" => type, "message" => message, "cursor" => cursor, "batch" => batch}}
  end

  def dump(_), do: :error

  def load(nil), do: {:ok, nil}

  def load(%{"type" => type, "message" => message, "cursor" => cursor, "batch" => batch}) do
    {:ok,
     %PartitionError{type: String.to_atom(type), message: message, cursor: cursor, batch: batch}}
  end

  def load(_), do: :error
end
