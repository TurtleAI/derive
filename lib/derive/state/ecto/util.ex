defmodule Derive.State.Ecto.Util do
  import Ecto.Query

  def selector_query([type, id]) when is_atom(type) and is_binary(id) do
    [primary_key] = type.__schema__(:primary_key)
    fields = [{primary_key, id}]
    from(rec in type, where: ^fields)
  end

  def selector_query([type, fields]) when is_atom(type) and is_list(fields) do
    from(rec in type, where: ^fields)
  end

  def array_push_uniq_query(%Ecto.Multi{} = multi, index, [type, id], attr, value) do
    query = from(rec in selector_query([type, id]), where: ^value not in field(rec, ^attr))
    Ecto.Multi.update_all(multi, index, query, push: [{attr, value}])
  end
end
