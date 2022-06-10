defmodule Derive.Ext.GenServer do
  @type reply :: {:ok, term} | {:error, :timeout}

  @type server_with_message :: {GenServer.server(), term}
  @type server_with_response :: {GenServer.server(), reply}

  @doc """
  Given list of {server, message} tuples, send a `GenServer.call/2 to each one
  with the given message and returns with the responses of each one.

  The returned tuples will always match the structure as the requests, except
  the send item will be either {:ok, value} or a timeout error {:error, :timeout}
  """
  @spec call_many([server_with_message()], timeout) :: [server_with_response()]
  def call_many(servers_with_messages, timeout \\ 5000) do
    servers_with_requests =
      for {server, message} <- servers_with_messages do
        {server, :gen_server.send_request(server, message)}
      end

    # To avoid waiting (num_calls * timeout) rather than timeout,
    # we can check the inbox for :overall_timeout after each request
    # to see if we've exceeded the overall timeout
    timer_ref = Process.send_after(self(), :overall_timeout, timeout)

    # the accumulator is of the form:
    # {:ok | :overall_timeout, [{server1, response1}, {server2, response2}, ...]}
    # Once the status becomes :overall_timeout, we consider any pending requests as timed out
    {_status, servers_with_replies} = process_replies(servers_with_requests, {:ok, []}, timeout)

    Process.cancel_timer(timer_ref)

    Enum.reverse(servers_with_replies)
  end

  defp process_replies([], {status, items}, _timeout),
    do: {status, items}

  defp process_replies([{sub, req_id} | rest], {status, items}, timeout) do
    case :gen_server.receive_response(req_id, timeout) do
      {:reply, reply} ->
        process_replies(rest, {status, [{sub, {:ok, reply}} | items]}, timeout)

      # The GenServer died before a reply was sent
      {:error, reason} ->
        process_replies(rest, {status, [{sub, {:error, reason}} | items]}, timeout)

      # We exceeded the overall timeout for this individual call
      # So we can globally consider this a timeout and process the rest without any more waiting
      :timeout ->
        process_replies(rest, {:overall_timeout, [{sub, {:error, :timeout}} | items]}, 0)
    end
  end
end
