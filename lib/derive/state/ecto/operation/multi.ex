defimpl Derive.State.Ecto.DbOp, for: Ecto.Multi do
  @moduledoc """
  Pass-through implementation for an `Ecto.Multi`
  This lets us drop down to using plain `Ecto.Multi` over implementations of
  `Derive.State.Ecto.DbOp`
  """

  def to_multi(%Ecto.Multi{} = multi, _index),
    do: multi
end
