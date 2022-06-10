defmodule Derive.Ex.GenServerTest do
  use ExUnit.Case

  import Derive.Ext.GenServer

  defmodule Server do
    use GenServer

    def start_link(opts \\ []),
      do: GenServer.start_link(__MODULE__, nil, opts)

    def init(nil),
      do: {:ok, nil}

    def handle_call({reply, sleep}, _from, state) do
      Process.sleep(sleep)
      {:reply, reply, state}
    end
  end

  @timeout 100

  def start(n) do
    for _ <- 1..n do
      {:ok, pid} = Server.start_link()
      pid
    end
  end

  def call_timed(servers_with_messages, timeout) do
    {elapsed, messages} = :timer.tc(fn -> call_many(servers_with_messages, timeout) end)
    # convert microseconds to milliseconds to matche Process.sleep units
    {div(elapsed, 1000), messages}
  end

  describe "Derive.Ext.GenServer.call_many" do
    test "awaiting multiple processes" do
      [p1, p2] = start(2)

      assert [{^p1, {:ok, :p1hi}}, {^p2, {:ok, :p2hi}}] =
               call_many([{p1, {:p1hi, 10}}, {p2, {:p2hi, 50}}], @timeout)
    end

    test "one process that takes too long" do
      [p1, p2] = start(2)

      assert [{^p1, {:error, :timeout}}, {^p2, {:ok, :p2hi}}] =
               call_many([{p1, {:p1hi, 100}}, {p2, {:p2hi, 10}}], 50)
    end

    test "several processes take too long" do
      [p1, p2, p3] = start(3)

      assert {elapsed, [{^p1, {:error, :timeout}}, {^p2, {:error, :timeout}}, {^p3, {:ok, :yay}}]} =
               call_timed([{p1, {:p1hi, 100}}, {p2, {:p2hi, 75}}, {p3, {:yay, 25}}], 50)

      assert elapsed < 100
    end
  end
end
