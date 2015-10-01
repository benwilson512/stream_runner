defmodule StreamRouterTest do
  use ExUnit.Case, async: false

  setup_all do
    {:ok, _} = Application.ensure_all_started(:logger)
    :ok
  end

  test "start_link/1" do
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
end
