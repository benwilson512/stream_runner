defmodule TestSink do
  defstruct pid: nil, forward_to: nil, val: nil
  use StreamRouter

  def start_link(args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def init({val, forward}) do
    {:ok, %__MODULE__{val: val, forward_to: forward}}
  end

  def handle_info({:stream_up, source}, state) do
    StreamRouter.ask_async(source, 1)
    {:noreply, state}
  end

  def handle_event({:eos, _}, _, %{forward_to: forward} = state) do
    send forward, :eos
    {:noreply, state}
  end

  def handle_event(new_val, source, state) do
    StreamRouter.ask_async(source, 1)
    send(state.forward_to, {:event, new_val})
    {:noreply, %{state | val: new_val}}
  end

  def handle_info(_, state), do: {:noreply, state}

end

defmodule StreamRouterTest do
  use ExUnit.Case, async: true

  setup_all do
    {:ok, _} = Application.ensure_all_started(:logger)
    :ok
  end

  test "start_link/1, ask/1, ask_async/1" do
    self = self()
    {:ok, pid} = 1..10
    |> Stream.each(&send(self, &1))
    |> StreamRouter.start_link()

    pid
    |> StreamRouter.ask_async(1)
    assert_receive 1

    pid
    |> StreamRouter.ask_async(1)
    assert_receive 2

    pid
    |> StreamRouter.ask(2)

    assert_receive 3
    assert_receive 4
  end

  test "stream slows to slowest subscriber" do
    test_pid  = self()
    other_sub = spawn(__MODULE__, :greedy_subscriber, [test_pid])

    {:ok, pid} = 1..10
    |> Stream.each(&send(test_pid, {:test_pid, &1}))
    |> Stream.each(&send(other_sub, {:other_sub, &1}))
    |> StreamRouter.start_link()

    send(other_sub, {:source, pid})

    pid
    |> StreamRouter.ask_async(1)

    assert_receive {:test_pid, 1}
    assert_receive {:other_sub, 1}
    refute_receive _

  end

  test "macro stuff works" do
    range = 1..5

    {:ok, sink_pid} = TestSink.start_link({0, self()})

    {:ok, _} = range
    |> Stream.into(%TestSink{pid: sink_pid})
    |> StreamRouter.start_link()

    for i <- range do
      assert_receive {:event, ^i}
    end
    assert_receive :eos
    refute_receive _
  end

  def greedy_subscriber(test_pid) do
    receive do
      {:source, source} ->
        StreamRouter.ask(source, 1)
        do_greedy_subscriber(test_pid, source)
    end
  end

  def do_greedy_subscriber(test_pid, source) do
    receive do
      value ->
        send(test_pid, value)
        StreamRouter.ask(source, 1)
    end
  end
end
