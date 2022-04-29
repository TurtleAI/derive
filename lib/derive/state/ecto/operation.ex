defmodule Derive.State.Ecto.Operation do
  @moduledoc """
  Provides shortcut functions for common Ecto state operations to reduce
  the amount of boilerplate that needs to be written for reducers.

  For example, rather than creating `%Operation.Insert{record: record}`, you can just call
  `insert(record)`.
  """

  alias Derive.State.Ecto.Operation

  def insert(record),
    do: %Operation.Insert{record: record}

  def insert_new(record),
    do: %Operation.Insert{record: record, on_conflict: :nothing}

  def update(selector, fields),
    do: %Operation.Update{selector: selector, fields: fields}

  def upsert(record, on_conflict),
    do: %Operation.Insert{record: record, on_conflict: on_conflict}

  def delete(selector),
    do: %Operation.Delete{selector: selector}

  def inc(selector, field, delta),
    do: %Operation.Increment{selector: selector, field: field, delta: delta}

  def merge(selector, fields),
    do: %Operation.Merge{selector: selector, fields: Enum.into(fields, %{})}

  def replace(record),
    do: %Operation.Replace{record: record}

  def array_push_uniq(selector, field, values) do
    %Operation.ArrayPush{
      unique: true,
      selector: selector,
      field: field,
      values: List.wrap(values)
    }
  end

  def array_delete(selector, field, value),
    do: %Operation.ArrayDelete{selector: selector, field: field, value: value}

  def transaction(fun) when is_function(fun, 1),
    do: %Operation.Transaction{fun: fun}

  def trace(func) when is_function(func, 1) do
    transaction(fn repo ->
      func.(repo)
      []
    end)
  end
end
