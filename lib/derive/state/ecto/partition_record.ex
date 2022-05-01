defmodule Derive.State.Ecto.PartitionRecord do
  @moduledoc """
  Each reducer has a table to keep track of partitions,
  up to which event they have processed, and whether it's in an
  active or error state.

  This is model that backs it.
  """

  use Derive.State.Ecto.Model

  @primary_key {:id, :string, [autogenerate: false]}
  schema "partitions" do
    field(:cursor, :string)
    field(:status, Ecto.Enum, values: [ok: 1, error: 2])
  end

  # Because we can't create a migration with a dynamic table name using create table(...),
  # we implement the raw up_sql/down_sql implementations instead
  def up_sql(table) do
    # Equivalent up/0 implementation
    # create table(:partitions, primary_key: false) do
    #   add(:id, :string, size: 32, primary_key: true)
    #   add(:cursor, :string, size: 32)
    #   add(:status, :integer, null: false, default: 1)
    # end

    [
      """
      CREATE TABLE #{table} (
        id character varying(32) PRIMARY KEY,
        cursor character varying(32),
        status integer NOT NULL DEFAULT 1
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
