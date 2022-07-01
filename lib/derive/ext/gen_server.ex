defmodule Derive.Ext.GenServer do
  @type reply :: {:reply, term} | {:error, :timeout}

  @type server_with_message ::
          {GenServer.server(), term} | {message_key(), GenServer.server(), message()}

  @type keyed_response :: {message_key(), reply}

  @type message_key :: any()
  @type message :: any()

  @doc """
  Given list of {key, server, message} tuples, send a `GenServer.call/2 to each one
  with the given message and returns with the responses of each in the form {key, reply}

  If key is omitted like {server, message}, it will default to the server itself.

  The returned tuples will correspond to the requests.
  So `[{server1, message1}, {server2, message2}]` would come back with [{server1, reply1}, {server2, reply2}]
  """
  @spec call_many([server_with_message()], timeout) :: [keyed_response()]
  def call_many(servers_with_messages, timeout \\ 5000) do
    servers_with_requests =
      servers_with_messages
      |> Enum.map(fn
        {key, server, message} -> {key, server, message}
        {server, message} -> {server, server, message}
      end)
      |> Enum.map(fn {key, server, message} ->
        {key, :gen_server.send_request(server, message)}
      end)

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

  defp process_replies([{key, req_id} | rest], {status, items}, timeout) do
    case :gen_server.receive_response(req_id, timeout) do
      {:reply, reply} ->
        process_replies(rest, {status, [{key, {:reply, reply}} | items]}, timeout)

      # The GenServer died before a reply was sent
      {:error, reason} ->
        process_replies(rest, {status, [{key, {:error, reason}} | items]}, timeout)

      # We exceeded the overall timeout for this individual call
      # By dropping the timeout to 0, we can collect the responses that have completed successfully so far
      # and considered the rest as timed out.
      :timeout ->
        process_replies(rest, {:overall_timeout, [{key, {:error, :timeout}} | items]}, 0)
    end
  end
end
