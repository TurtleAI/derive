defmodule Derive.State.Ecto.Selector do
  @moduledoc """
  A selector is a tuple that points rows in a database.

  For example:
  * `{MyApp.User, 5}` could refer to a user with id 5.
  * `{MyApp.Checkins, user_id: 5, location_id: 23}` could refer to the checkin with the matching fields

  Most operations that implement `Derive.State.Ecto.DbOp` accept selectors as a way to identify
  records in the database.
  """

  @typedoc """
  An identifier for a record or many a list of records
  Can be a single identifier or a list of fields
  """
  @type id :: number() | binary() | keyword()

  @type t :: {Ecto.Schema.t(), id()}

  import Ecto.Query

  def selector_query({type, id}) when is_atom(type) and is_binary(id) do
    [primary_key] = type.__schema__(:primary_key)
    fields = [{primary_key, id}]
    from(rec in type, where: ^fields)
  end

  def selector_query({type, fields}) when is_atom(type) and is_list(fields) do
    from(rec in type, where: ^fields)
  end
end
