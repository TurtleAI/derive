defmodule Derive.Ecto.State do
  @moduledoc """
  An Ecto-based implementation of state
  """

  defstruct [:repo, :namespace, :models, version: "1"]

  @type t :: %__MODULE__{
          repo: Ecto.Repo.t(),
          namespace: binary(),
          models: [Derive.Ecto.Model.t()],
          version: binary()
        }

  require Logger

  alias __MODULE__, as: S

  alias Derive.Partition
  alias Derive.Ecto.PartitionRecord
  alias Derive.MultiOp
  alias Derive.Error.CommitError

  @doc """
  Commit a list of operations to disk within a transaction.
  """
  @spec commit(t(), MultiOp.t()) :: MultiOp.t()
  def commit(%S{repo: repo} = state, %MultiOp{partition: partition} = multi_op) do
    multi_op =
      MultiOp.save_partition(multi_op, %Derive.Ecto.Operation.SetPartition{
        table: partition_table(state),
        partition: partition
      })

    operations = MultiOp.operations(multi_op) ++ [multi_op.save_partition]

    multi = operations_to_multi(Ecto.Multi.new(), 0, operations)

    case safe_transaction(repo, multi) do
      {:ok, _changes} ->
        MultiOp.committed(multi_op)

      {:error, failed_operation_index, %Ecto.Changeset{errors: errors}, _changes_so_far} ->
        failed_event_op = MultiOp.find_event_op_by_index(multi_op, failed_operation_index)

        commit_error = %CommitError{
          commit: {__MODULE__, :commit},
          operations: MultiOp.event_operations(multi_op),
          failed_operation: failed_event_op,
          error: errors
        }

        multi_op = MultiOp.failed(multi_op, commit_error)
        save_partitions(state, [multi_op.partition])
        multi_op

      {:exception, error, stacktrace} ->
        commit_error = %CommitError{
          commit: {__MODULE__, :commit},
          operations: MultiOp.event_operations(multi_op),
          error: error,
          stacktrace: stacktrace
        }

        multi_op = MultiOp.failed(multi_op, commit_error)
        save_partitions(state, [multi_op.partition])
        multi_op
    end
  end

  # Execute a transaction but capture an exception and return it as an exception tuple
  # This allows to explicitly handle an error without crashing the process
  @spec safe_transaction(module(), Ecto.Multi.t()) ::
          {:ok, any}
          | {:error, any}
          | {:error, Ecto.Multi.name(), any, %{Ecto.Multi.name() => any}}
          | {:exception, any(), Exception.stacktrace()}
  defp safe_transaction(repo, multi) do
    try do
      repo.transaction(multi)
    rescue
      error ->
        {:exception, error, __STACKTRACE__}
    end
  end

  def needs_rebuild?(%S{repo: repo, version: target_version} = state) do
    # If a table DOES NOT exist, we immediately know a rebuild is needed
    # we can skip over having to raise and catch a postgres error which can be problematic
    # in transaction
    if table_exists?(repo, partition_table(state)) do
      resp =
        try do
          {:ok, load_partition(state, Partition.version_id())}
        catch
          # in some cases, the partition table doesn't exist yet
          # in that case, we want to return true
          :error, %Postgrex.Error{postgres: %{code: code}}
          when code in [:undefined_table, :undefined_column] ->
            {:error, :missing_table}
        end

      case resp do
        {:ok, %Partition{cursor: stored_version}} ->
          target_version != stored_version

        {:error, :missing_table} ->
          true
      end
    else
      true
    end
  end

  defp table_exists?(repo, table_name) do
    case Ecto.Adapters.SQL.query!(repo, "SELECT to_regclass($1)", [table_name]) do
      %Postgrex.Result{rows: [[nil]]} ->
        false

      %Postgrex.Result{rows: [[_oid]]} ->
        true
    end
  end

  def load_partition(%S{} = state, partition_id) do
    case get_partition_record(state, partition_id) do
      nil ->
        %Partition{
          id: partition_id,
          cursor: :start,
          status: :ok
        }

      %PartitionRecord{} = record ->
        PartitionRecord.to_partition(record)
    end
  end

  defp get_partition_record(%S{repo: repo} = state, partition_id) do
    repo.get({partition_table(state), PartitionRecord}, partition_id)
  end

  def save_partitions(%S{repo: repo} = state, partitions) do
    partitions
    |> Enum.map(fn partition ->
      %Derive.Ecto.Operation.SetPartition{
        table: partition_table(state),
        partition: partition
      }
    end)
    |> operations_to_multi()
    |> repo.transaction()

    :ok
  end

  defp partition_table(%S{namespace: namespace}),
    do: "#{namespace}_partitions"

  @doc """
  Erase all of the state to bring it back to the starting point.
  This involves dropping and recreating all the tables including the partition tables.
  """
  @spec reset_state(t(), [Partition.t()]) :: :ok
  def reset_state(%S{} = state, initial_partitions \\ []) do
    clear_state(state)
    init_state(state, initial_partitions)
  end

  def clear_state(%S{repo: repo, models: models} = state) do
    models = [{PartitionRecord, partition_table(state)} | models]
    for model <- models, do: clear_model(model, repo)
  end

  @doc """
  Setup the tables so that a Derive process can properly function
  to update the current state.
  """
  @spec init_state(t(), [Partition.t()]) :: :ok
  def init_state(
        %S{repo: repo, models: models, version: version} = state,
        initial_partitions \\ []
      ) do
    models = [{PartitionRecord, partition_table(state)} | models]
    for model <- models, do: init_model(model, repo)

    save_partitions(state, [
      %Partition{id: Partition.version_id(), cursor: version, status: :ok} | initial_partitions
    ])
  end

  defp clear_model(model, repo) when is_atom(model) do
    run_migration(repo, [], 0, model, :forward, :down, :down, log: :debug)
  end

  # if a model has a custom down_sql we can use that instead
  defp clear_model({model, opts}, repo) when is_atom(model) do
    for q <- model.down_sql(opts), do: repo.query!(q)
  end

  defp init_model(model, repo) when is_atom(model) do
    run_migration(repo, [], 1, model, :forward, :up, :up, log: :debug)
  end

  # if a model has a custom down_sql or up_sql, we can use that instead
  defp init_model({model, opts}, repo) when is_atom(model) do
    for q <- model.up_sql(opts), do: repo.query!(q)
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

  defp operations_to_multi(operations),
    do: operations_to_multi(Ecto.Multi.new(), 0, operations)

  defp operations_to_multi(multi, _, []), do: multi

  defp operations_to_multi(multi, index, [op | rest]) do
    multi
    |> Ecto.Multi.append(Derive.Ecto.DbOp.to_multi(op, index))
    |> operations_to_multi(index + 1, rest)
  end
end
