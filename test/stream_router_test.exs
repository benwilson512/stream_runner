defmodule StreamRouterTest do
  use ExUnit.Case, async: false

  setup_all do
    {:ok, _} = Application.ensure_all_started(:logger)
    :ok
  end

  test "start_link/1" do
    self = self()
    {:ok, pid} = StreamRouter.start_link(1..10, self)
    assert pid |> Process.alive?

    pid |> send({:"$gen_ask", {self(), :foo}, {1, []}})
    assert_receive {:"$gen_route", {^self, nil}, [1]}

    pid |> send({:"$gen_ask", {self(), :foo}, {2, []}})
    assert_receive {:"$gen_route", {^self, nil}, [2]}
    assert_receive {:"$gen_route", {^self, nil}, [3]}
  end
end
