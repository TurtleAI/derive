defmodule Derive.State.Ecto.PartitionRecord do
  use Derive.State.Ecto.Model

  @primary_key {:id, :string, [autogenerate: false]}
  schema "partitions" do
    field(:version, :string)
    field(:status, Ecto.Enum, values: [ok: 1, error: 2])
  end

  def up_sql(table) do
    [
      """
      CREATE TABLE #{table} (
        id character varying(32) PRIMARY KEY,
        version character varying(32),
        status integer NOT NULL DEFAULT 1
      );
      """
    ]
  end

  def down_sql(table) do
    [
      "DROP TABLE IF EXISTS #{table};"
    ]
  end

  def up do
    create table(:partitions, primary_key: false) do
      add(:id, :string, size: 32, primary_key: true)
      add(:version, :string, size: 32)
      add(:status, :integer, null: false, default: 1)
    end
  end

  def down do
    drop_if_exists(table(:partitions))
  end
end
