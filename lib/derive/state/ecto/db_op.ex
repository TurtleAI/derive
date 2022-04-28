defprotocol Derive.State.Ecto.DbOp do
  @moduledoc """
  Represents an operation which updates the state of an Ecto database,
  such as insert, update and delete.

  These get translated into regular `Ecto.Multi` structs before they are committed.

  For cases where the built-in operations aren't sufficient,
  you can implement your own structs that get translated into db operations.

  Or you can drop down to `Ecto.Multi` or `Ecto.Query` (for an update_all query)
  Both of these implement this protocol.
  """

  @spec to_multi(t, Ecto.Multi.name()) :: Ecto.Multi.t()
  def to_multi(op, name)
end
