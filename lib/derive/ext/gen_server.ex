defmodule Derive.Ext.GenServer do
  @type reply :: {:reply, term} | {:error, :timeout}

  @type key_server_message :: {message_key(), GenServer.server(), message()}

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
  @spec call_many([key_server_message()], timeout) :: [keyed_response()]
  def call_many(key_server_messages, timeout \\ 5000) do
    start_time = :erlang.monotonic_time(:millisecond)
    deadline = start_time + timeout

    responses_by_keys =
      :gen_server.reqids_new()
      |> send_requests(key_server_messages)
      |> receive_responses({:abs, deadline})
      |> Enum.into(%{})

    for {key, _server, _message} <- key_server_messages do
      {key, responses_by_keys[key]}
    end
  end

  defp send_requests(request_ids, []),
    do: request_ids

  defp send_requests(request_ids, [{key, server, message} | rest_calls]) do
    new_request_ids = :gen_server.send_request(server, message, key, request_ids)
    send_requests(new_request_ids, rest_calls)
  end

  defp receive_responses(request_ids, timeout) do
    case :gen_server.receive_response(request_ids, timeout, true) do
      :no_request ->
        []

      :timeout ->
        for {_req_id, label} <- :gen_server.reqids_to_list(request_ids) do
          {label, :timeout}
        end

      {response, label, new_req_ids} ->
        [{label, response} | receive_responses(new_req_ids, timeout)]
    end
  end
end
