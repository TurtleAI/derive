defmodule Derive.State.Ecto.Operation do
  @moduledoc """
  Provides shortcut functions for common Ecto state operations to reduce
  the amount of boilerplate that needs to be written for reducers.

  For example, rather than creating `%Operation.Insert{record: record}`, you can just call
  `insert(record)`.
  """

  alias Derive.State.Ecto.Operation

  def insert(record), do: %Operation.Insert{record: record}
  def insert_if_missing(record), do: %Operation.Insert{record: record, on_conflict: :nothing}

  def update(selector, attrs), do: %Operation.Update{selector: selector, attrs: attrs}

  def upsert(record, on_conflict), do: %Operation.Insert{record: record, on_conflict: on_conflict}

  def delete(selector), do: %Operation.Delete{selector: selector}

  def inc(selector, attr, delta),
    do: %Operation.Increment{selector: selector, attr: attr, delta: delta}

  def merge(%type{} = record) do
    primary_key = type.__schema__(:primary_key)
    merge([type, Map.get(record, primary_key)], record)
  end

  def merge(selector, attrs) do
    %Operation.Merge{selector: selector, attrs: attrs}
  end

  def array_push_uniq(selector, attr, values) do
    %Operation.ArrayPush{unique: true, selector: selector, attr: attr, values: List.wrap(values)}
  end

  def array_delete(selector, attr, value) do
    %Operation.ArrayDelete{selector: selector, attr: attr, value: value}
  end

  def transaction(fun) when is_function(fun, 1) do
    %Operation.Transaction{fun: fun}
  end

  def trace(func) when is_function(func, 1) do
    transaction(fn repo ->
      func.(repo)
      []
    end)
  end
end
