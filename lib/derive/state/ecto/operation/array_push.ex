defmodule Derive.State.Ecto.Operation.ArrayPush do
  defstruct [:selector, :attr, :values, unique: false]
end
