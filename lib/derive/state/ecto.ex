defmodule Derive.State.Ecto do
  @moduledoc """
  An Ecto-based implementation of state
  """

  defstruct [:repo, :namespace, :models]

  @type t :: %__MODULE__{
          repo: Ecto.Repo.t(),
          namespace: binary(),
          models: [Derive.State.Ecto.Model.t()]
        }

  alias __MODULE__, as: S

  alias Derive.State.Ecto.PartitionRecord
  alias Derive.State.MultiOp

  @doc """
  Commit a list of operations to disk within a transaction.
  """
  def commit(%S{repo: repo} = state, %MultiOp{} = op) do
    operations =
      MultiOp.operations(op) ++
        [
          %Derive.State.Ecto.Operation.SetPartition{
            table: partition_table(state),
            partition: op.partition
          }
        ]

    multi = operations_to_multi(Ecto.Multi.new(), 1, operations)

    try do
      repo.transaction(multi)
      MultiOp.committed(op)
    rescue
      error ->
        MultiOp.commit_failed(op, error)
    end
  end

  def get_partition(%S{repo: repo} = state, partition_id) do
    case repo.get({partition_table(state), PartitionRecord}, partition_id) do
      nil ->
        %Derive.Partition{
          id: partition_id,
          cursor: :start,
          status: :ok
        }

      %{id: id, cursor: cursor, status: status} ->
        %Derive.Partition{
          id: id,
          cursor: cursor,
          status: status
        }
    end
  end

  def set_partition(%S{repo: repo} = state, partition) do
    multi =
      operations_to_multi(Ecto.Multi.new(), 1, [
        %Derive.State.Ecto.Operation.SetPartition{
          table: partition_table(state),
          partition: partition
        }
      ])

    repo.transaction(multi)
  end

  defp partition_table(%S{namespace: namespace}),
    do: "#{namespace}_partitions"

  @doc """
  Erase all of the state to bring it back to the starting point.
  This involves dropping and recreating all the tables.
  """
  def reset_state(%S{repo: repo, models: models} = state) do
    models = [
      {PartitionRecord, partition_table(state)} | models
    ]

    for model <- models do
      reset_model(model, repo)
    end
  end

  defp reset_model(model, repo) when is_atom(model) do
    run_migration(repo, [], 0, model, :forward, :down, :down, log: :debug)
    run_migration(repo, [], 1, model, :forward, :up, :up, log: :debug)
  end

  # if a model has a custom down_sql or up_sql, we can use that instead
  defp reset_model({model, opts}, repo) when is_atom(model) do
    for q <- model.down_sql(opts) ++ model.up_sql(opts) do
      repo.query!(q)
    end
  end

  # Copy of `Ecto.Migation.Runner.run`
  # The default implementation links the process with self(), which causes
  # the calling process to shutdown with the migration runner when the migration is done
  # For Derive, we want the process to keep running
  defp run_migration(
         repo,
         config,
         _version,
         module,
         direction,
         operation,
         migrator_direction,
         opts
       ) do
    level = Keyword.get(opts, :log, :info)
    sql = Keyword.get(opts, :log_migrations_sql, false)
    log = %{level: level, sql: sql}
    args = {self(), repo, config, module, direction, migrator_direction, log}

    {:ok, runner} =
      DynamicSupervisor.start_child(Ecto.MigratorSupervisor, {Ecto.Migration.Runner, args})

    # We undo the `Process.link` called in `&Ecto.Migration.Runner.start_link/1`
    # to ensure self() isn't shut dowon once the migration completes
    Process.unlink(runner)

    Ecto.Migration.Runner.metadata(runner, opts)

    apply(module, operation, [])

    Ecto.Migration.Runner.flush()
    Ecto.Migration.Runner.stop()
  end

  defp operations_to_multi(multi, _, []), do: multi

  defp operations_to_multi(multi, index, [op | rest]) do
    multi
    |> Ecto.Multi.append(Derive.State.Ecto.DbOp.to_multi(op, index))
    |> operations_to_multi(index + 1, rest)
  end
end
