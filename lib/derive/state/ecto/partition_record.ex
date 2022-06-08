defmodule Derive.State.Ecto.PartitionRecord do
  @moduledoc """
  Each reducer has a table to keep track of partitions,
  up to which event they have processed, and whether it's in an
  active or error state.

  This is model that backs it.
  """

  use Derive.State.Ecto.Model

  alias Derive.Partition

  @primary_key {:id, :string, [autogenerate: false]}
  schema "partitions" do
    field(:cursor, Derive.State.Ecto.CursorType)
    field(:status, Ecto.Enum, values: [ok: 1, error: 2])
    field(:error, Derive.State.Ecto.PartitionErrorType)
  end

  def from_partition(%Partition{id: id, cursor: cursor, status: status, error: error}) do
    %__MODULE__{id: id, cursor: cursor, status: status, error: error}
  end

  def to_partition(%__MODULE__{id: id, cursor: cursor, status: status, error: error}) do
    %Partition{
      id: id,
      cursor: cursor,
      status: status,
      error: error
    }
  end

  # Because we can't create a migration with a dynamic table name using create table(...),
  # we implement the raw up_sql/down_sql implementations instead
  def up_sql(table) do
    # Equivalent up/0 implementation
    # create table(:partitions, primary_key: false) do
    #   add(:id, :string, size: 32, primary_key: true)
    #   add(:cursor, :string, size: 32)
    #   add(:status, :integer, null: false, default: 1)
    #   add(:meta, :map)
    # end

    [
      """
      CREATE TABLE IF NOT EXISTS #{table} (
        id character varying(32) PRIMARY KEY,
        cursor character varying(32),
        status integer NOT NULL DEFAULT 1,
        error jsonb
      );
      """
    ]
  end

  def down_sql(table) do
    # Equivalent down/0 implementation
    # drop_if_exists(table(:partitions))
    [
      "DROP TABLE IF EXISTS #{table};"
    ]
  end
end
