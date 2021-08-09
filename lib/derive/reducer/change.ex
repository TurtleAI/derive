defmodule Derive.Reducer.Change.Put do
  defstruct [:selector, :record]
end

defmodule Derive.Reducer.Change.Delete do
  defstruct [:selector]
end

defmodule Derive.Reducer.Change.Update do
  defstruct [:selector, :attrs]
end

defmodule Derive.Reducer.Change.Merge do
  defstruct [:selector, :attrs]
end

defmodule Derive.Reducer.Change.Reset do
  defstruct []
end

defmodule Derive.Reducer.Change do
  def put(selector, record),
    do: %Derive.Reducer.Change.Put{selector: selector, record: record}

  def merge(selector, attrs),
    do: %Derive.Reducer.Change.Merge{selector: selector, attrs: attrs}

  def delete(selector),
    do: %Derive.Reducer.Change.Delete{selector: selector}
end
