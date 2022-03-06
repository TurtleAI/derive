defmodule Derive.State.InMemory.Operation do
  alias Derive.State.InMemory.Operation

  def put(selector, record),
    do: %Operation.Put{selector: selector, record: record}

  def merge(selector, attrs),
    do: %Operation.Merge{selector: selector, attrs: attrs}

  def delete(selector),
    do: %Operation.Delete{selector: selector}
end
