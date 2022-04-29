# Derive

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

- [ ] Support rebuilding very large reducers
- [ ] Render progress when rebuilding large reducers
- [x] Error handling when a commit fails
- [x] Add tests for Ecto DB operations
- [ ] Create a Derive.EctoReducer with reduced boilerplate
- [ ] Shutdown Derive.PartitionDispatcher after a period of inactivity
- [x] Improved logging of operations, commits, etc
- [ ] Integrate with turtle-api
