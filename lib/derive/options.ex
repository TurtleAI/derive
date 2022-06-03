defmodule Derive.Options do
  @moduledoc """
  Contains all of the options necessary to start a Derive process

  Note:
  Once a Derive process boots, these options will not change.
  """

  defstruct [
    :name,
    :source,
    :show_progress,
    :validate_version,
    :reducer,
    :batch_size,
    :mode,
    :logger
  ]

  @type t :: %__MODULE__{
          name: atom(),
          source: any(),
          reducer: Derive.Reducer.t(),
          batch_size: non_neg_integer(),
          mode: mode(),
          logger: Derive.Logger.t()
        }

  @type option ::
          {:reducer, Reducer.t()}
          | {:batch_size, non_neg_integer()}
          | {:mode, Derive.Options.mode()}
          | {:logger, Derive.Logger.t()}

  @typedoc """
  The mode in which the dispatcher should operate

  catchup: on boot, catch up to the last point possible, stay alive and up to date
  rebuild: destroy the state, rebuild it up to the last point possible, then shut down
  """
  @type mode :: :catchup | :rebuild
end
