defmodule Derive.Util.Partition do
  def partition_to_string({mod, id}) do
    mod_to_string!(mod) <> ":" <> id
  end

  defp mod_to_string!(mod) when is_atom(mod) do
    case mod_to_string(mod) do
      {:ok, str} -> str
      {:error, err} -> raise ArgumentError, err
    end
  end

  defp mod_to_string(mod) when is_atom(mod) do
    case Kernel.to_string(mod) do
      "Elixir." <> mod_str -> {:ok, mod_str}
      _any -> {:error, "Invalid module #{mod}"}
    end
  end
end
