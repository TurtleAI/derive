defmodule Derive.State.Ecto do
  @moduledoc """
  An Ecto-based implementation of state
  """

  defstruct [:repo, :namespace]

  alias __MODULE__, as: S
  alias Drive.State.Ecto.PartitionRecord

  @doc """
  Commit a list of operations to disk within a transaction.
  """
  def commit(%S{repo: repo}, operations) do
    multi = operations_to_multi(Ecto.Multi.new(), 1, operations)
    repo.transaction(multi)
  end

  def get_partition(%S{repo: repo} = state, partition_id) do
    case repo.get({partition_table(state), PartitionRecord}, partition_id) do
      nil ->
        %Derive.Partition{
          id: partition_id,
          version: :start,
          status: :ok
        }

      %{id: id, version: version, status: status} ->
        %Derive.Partition{
          id: id,
          version: version,
          status: status
        }
    end
  end

  def set_partition(%S{} = state, partition) do
    Derive.State.Ecto.commit(state, [
      %Derive.State.Ecto.Operation.SetPartition{
        table: partition_table(state),
        partition: partition
      }
    ])
  end

  defp partition_table(%S{namespace: namespace}),
    do: "#{namespace}_partitions"

  @doc """
  Erase all of the state to bring it back to the starting point.
  This involves dropping and recreating all the tables.
  """
  def reset_state(repo, models) do
    for model <- models do
      reset_model(model, repo)
    end
  end

  defp reset_model(model, repo) when is_atom(model) do
    Ecto.Migration.Runner.run(repo, [], 0, model, :forward, :down, :down, [])
    Ecto.Migration.Runner.run(repo, [], 1, model, :forward, :up, :up, [])
  end

  # if a model has a custom down_sql or up_sql, we can use that instead
  defp reset_model({model, opts}, repo) when is_atom(model) do
    for q <- model.down_sql(opts) ++ model.up_sql(opts) do
      repo.query!(q)
    end
  end

  defp operations_to_multi(multi, _, []), do: multi

  defp operations_to_multi(multi, index, [op | rest]) do
    multi
    |> Ecto.Multi.append(Derive.State.Ecto.DbOp.to_multi(op, index))
    |> operations_to_multi(index + 1, rest)
  end
end
