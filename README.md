# Derive

**DISCLAIMER: This is an early project used internally at our company. We make no guarantees about its stability. Use at your own risk!**

Derive provides the infrastructure to keep derived state up to date based on a source stream of events.
This is commonly used for event sourcing or redux patterns.

For now, the state can be stored in Ecto or in memory.

The state is eventually consistent, but there are functions which allow waiting until a reducer has caught up
to a certain point.

There are 3 different scenarios that Derive handles:

1. Rebuilding state from scratch
2. Incrementally keeping state up to date based on new events
3. Resuming in the event of a restart such as a server shut-down

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `derive` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:derive, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/derive](https://hexdocs.pm/derive).

## Basic usage

See the tests directory for now.

## TODO

- [x] Support rebuilding very large reducers
- [x] Render progress when rebuilding large reducers
- [x] Error handling when a commit fails
- [x] Add tests for Ecto DB operations
- [x] Create a Derive.EctoReducer with reduced boilerplate
- [x] Improved logging of operations, commits, etc
- [x] Rename version to cursor
- [x] Integrate with turtle-api
- [x] Allow event logs to become part of the Derive supervision tree (to minimize configuration)
- [x] Allow awaiting events in Derive after a command is executed
- [x] Shutdown Derive.PartitionDispatcher after a period of inactivity
- [x] Pattern for sharing event logs between multiple Derive instances
- [x] Make the code cursor agnostic
- [x] Test recovery in case of an unexpected shutdown
- [ ] Log errors for partitions in the database
- [ ] Support use cases like email notifications
