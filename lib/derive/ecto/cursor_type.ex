defmodule Derive.Ecto.CursorType do
  @moduledoc """
  Encapsulates a partition
  """

  use Ecto.Type

  def type, do: :string

  def cast(:start), do: {:ok, :start}
  def cast(id) when is_binary(id), do: {:ok, id}
  def cast(_), do: :error

  def dump(:start), do: {:ok, "0"}
  def dump(value) when is_binary(value), do: {:ok, value}
  def dump(_), do: :error

  def load("0"), do: {:ok, :start}
  def load(value), do: {:ok, value}
end
