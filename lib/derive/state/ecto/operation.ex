defmodule Derive.State.Ecto.Operation do
  @moduledoc """
  Provides shortcut functions for common Ecto state operations to reduce
  the amount of boilerplate that needs to be written for reducers.

  For example, rather than creating `%Operation.Insert{record: record}`, you can just call
  `insert(record)`.
  """

  alias Derive.State.Ecto.Operation

  @type record :: Ecto.Schema.t()
  @type operation :: Derive.Reducer.operation()
  @type selector :: Derive.State.Ecto.Selector.t()
  @type fields :: keyword()
  @type field_name :: atom()

  @doc """
  Insert a new record into the database
  """
  @spec insert(record()) :: operation()
  def insert(record),
    do: %Operation.Insert{record: record}

  @doc """
  Insert a record into the database if it's not there
  We decide if something is missing or not based on the primary key
  """
  @spec insert_new(record()) :: operation()
  def insert_new(record),
    do: %Operation.Insert{record: record, on_conflict: :nothing}

  @doc """
  Update the given fields for an existing record.
  If the record doesn't exist, nothing happens.
  """
  @spec update(selector(), fields()) :: operation()
  def update(selector, fields),
    do: %Operation.Update{selector: selector, fields: fields}

  @doc """
  Upsert a record into the database based on the primary keys.
  The on_conflict is identical to the Ecto on_conflict option.
  """
  @spec upsert(record(), fields()) :: operation()
  def upsert(record, on_conflict),
    do: %Operation.Insert{record: record, on_conflict: on_conflict}

  @doc """
  Delete a record for a given selector
  """
  @spec delete(selector()) :: operation()
  def delete(selector),
    do: %Operation.Delete{selector: selector}

  @doc """
  Increment the field of a record by a delta.
  Can be a positive or negative integer.
  """
  @spec inc(selector(), field_name(), integer()) :: operation()
  def inc(selector, field, delta),
    do: %Operation.Increment{selector: selector, field: field, delta: delta}

  @spec merge(selector(), fields()) :: operation()
  def merge(selector, fields),
    do: %Operation.Merge{selector: selector, fields: Enum.into(fields, %{})}

  @doc """
  Replace an entire record in the database based on its primary key.
  """
  @spec replace(record()) :: operation()
  def replace(record),
    do: %Operation.Replace{record: record}

  @spec array_push_uniq(selector(), field_name(), term()) :: operation()
  def array_push_uniq(selector, field, values) do
    %Operation.ArrayPush{
      unique: true,
      selector: selector,
      field: field,
      values: List.wrap(values)
    }
  end

  @spec array_delete(selector(), field_name(), term()) :: operation()
  def array_delete(selector, field, value),
    do: %Operation.ArrayDelete{selector: selector, field: field, value: value}

  @doc """
  Execute a transaction
  If the transaction returns one or more operations that implement `Derive.State.Ecto.DbOp`,
  these will be committed as part of the transaction.
  """
  def transaction(fun) when is_function(fun, 1),
    do: %Operation.Transaction{fun: fun}

  def trace(func) when is_function(func, 1) do
    transaction(fn repo ->
      func.(repo)
      []
    end)
  end
end
