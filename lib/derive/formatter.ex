defmodule Derive.Formatter do
  @doc """
  Convert a module to a string
  """
  @spec mod_to_string(atom()) :: binary()
  def mod_to_string(mod) when is_atom(mod) do
    case Kernel.to_string(mod) do
      "Elixir." <> mod_str -> mod_str
      _any -> raise ArgumentError, "Invalid module #{inspect(mod)}"
    end
  end
end
