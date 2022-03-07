defmodule Derive.State.Ecto.Operation do
  alias Derive.State.Ecto.Operation

  def insert(record), do: %Operation.Insert{record: record}
end
