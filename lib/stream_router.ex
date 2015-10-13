defmodule StreamRouter do
  @moduledoc """
  The `StreamRouter` provides a convenient way to run a stream as a process routing
  the items enumerated by the stream to other processes along the way.

  Whereas StreamRunner iterates through the stream as fast as the stream source
  will permit, StreamRouter only iterates through an item when asked.

  ## Example

  We're going to build a stream of random numbers, and route those into a
  gen server that will manage 5 timers set according to the numbers.

  Here's our stream:

      stream = fn -> :random.uniform(10) * 1000 end
      |> Stream.repeatedly
      |> Stream.each(&TimerServer.add_timer(TimerServer.pid, &1))

      {:ok, pid} = StreamRouter.start_link(stream)

  And our GenServer

      defmodule TimerServer do
        @desired_timers 5

        def pid, do: Process.whereis(__MODULE__)

        def add_timer(pid, value) do
          GenServer.call(pid, {:add_timer, value})
        end

        def init(stream_pid) do
          StreamRouter.ask(stream_pid, @desired_timers)
          {:ok, stream_pid}
        end

        def handle_call({:add_timer, value}, _from, stream_pid) do
          Process.send_after(self(), :ding, value)
          {:reply, timer, stream_pid}
        end

        def handle_info(:ding, stream_pid) do
          StreamRouter.ask(stream_pid, 1)
          {:noreply, stream_pid}
        end
      end
  """

  defmacro __using__(source) do
    quote do
      @source_key unquote(source)
      @before_compile unquote(__MODULE__)

      def handle_info({:"$gen_route", {pid, ref}, event}, state) do
        result = handle_event(event, pid, state)
        send(pid, {ref, :ok})
        result
      end
    end
  end

  defmacro __before_compile__(_) do
    quote [location: :keep] do

      defimpl Collectable do
        def into(%{pid: pid} = dest) do
          pid
          |> resolve
          |> send({:stream_up, self()})
          {dest, &do_into/2}
        end

        def do_into(%{pid: pid} = dest, {:cont, value}) do
          StreamRouter.route(pid, value)
          dest
        end
        def do_into(%{pid: pid} = dest, done_or_halt) do
          StreamRouter.eos(pid, done_or_halt)
          dest
        end

        defp resolve(pid) when is_pid(pid), do: pid
        defp resolve(pid), do: Process.whereis(pid)
      end
    end
  end

  ## StreamRouter API
  ###################

  @doc """
  Route an event to a sink
  """
  @spec route(pid, term, timeout) :: :ok | {:error, term}
  def route(sink, event, timeout \\ 5000) do
    ref = Process.monitor(sink)
    send(sink, {:"$gen_route", {self(), ref}, event})
    wait_for_reply(:route, ref, sink, event, timeout)
  end

  @typedoc "`eos/2,3` possible reasons"
  @type eos_reason :: :done | :halted | {:error, term}

  @doc """
  Indicate demand for items
  """
  @spec ask(pid, integer, timeout) :: :ok | {:error, term}
  def ask(source, quantity, timeout \\ 5000) do
    ref = Process.monitor(source)
    send(source, {:"$gen_ask", {self(), ref}, {quantity, []}})
    wait_for_reply(:ask, ref, source, quantity, timeout)
  end

  @doc """
  Notify a sink that the stream is terminating
  """
  @spec ask_async(pid, integer) :: :ok
  def ask_async(source, quantity) do
    send(source, {:"$gen_ask", {self(), nil}, {quantity, []}})
    :ok
  end

  @doc """
  Notify a sink that the stream is terminating
  """
  @spec eos(pid, eos_reason) :: :ok | {:error, term}
  def eos(sink, reason, timeout \\ 5000) do
    ref = Process.monitor(sink)
    send(sink, {:"$gen_route", {self(), ref}, {:eos, reason}})
    wait_for_reply(:eos, ref, sink, reason, timeout)
  end

  defp wait_for_reply(fun, ref, sink, event, timeout) do
    receive do
      {^ref, reply} ->
        Process.demonitor(ref, [:flush])
        reply
      {:DOWN, ^ref, _, _, :noconnection} ->
        mfa = {__MODULE__, :route, [sink, event, timeout]}
        exit({{:nodedown, node(sink)}, mfa})
      {:DOWN, ^ref, _, _, reason} ->
        exit({reason, {__MODULE__, fun, [sink, event, timeout]}})
    after
      timeout ->
        Process.demonitor(ref, [:flush])
        exit({:timeout, {__MODULE__, fun, [sink, event, timeout]}})
    end
  end

  ## Starting StreamRouter
  ## Everything from here until
  ## the loop functions is exactly the same as
  ## StreamRunner and should be refactored out
  ############################################

  @typedoc "Debug option values."
  @type debug_option :: :trace | :log | :statistics | {:log_to_file, Path.t}

  @typedoc "The name of the `StreamRunner`."
  @type name :: atom | {:global, term} | {:via, module, term}

  @typedoc "`StreamRunner` `start_link/2` or `start/2` option values."
  @type option ::
    {:debug, [debug_option]} |
    {:name, name} |
    {:timeout, timeout} |
    {:spawn_opt, Process.spawn_opt}

  @typedoc "`start_link/2` or `start/2` return values."
  @type on_start :: {:ok, pid} | :ignore | {:error, {:already_started, pid} | term}

  @doc """
  Start a `StreamRunner` as part of the supervision tree.
  """
  @spec start_link(Enumerable.t, [option]) :: on_start
  def start_link(stream, opts \\ []) do
    start(stream, opts, :link)
  end

  @doc """
  Start a `StreamRunner`.

  The `StreamRunner` is not linked to the calling process.
  """
  def start(stream, opts \\ []) do
    start(stream, opts, :nolink)
  end

  ## :gen callbacks common with

  @doc false
  def init_it(starter, :self, name, mod, args, opts) do
    init_it(starter, self(), name, mod, args, opts)
  end
  def init_it(starter, parent, name, __MODULE__, stream, opts) do
    _ = Process.put(:"$initial_call", {StreamRunner, :init_it, 6})
    dbg = :gen.debug_options(opts)
    try do
      Enumerable.reduce(stream, {:suspend, nil}, fn(v, _) -> {:suspend, v} end)
    catch
      :error, value ->
        reason = {value, System.stacktrace()}
        init_error(reason, starter, name)
      :throw, value ->
        reason = {{:nocatch, value}, System.stacktrace()}
        init_error(reason, starter, name)
      :exit, reason ->
        init_error(reason, starter, name)
    else
      {res, nil} when res in [:halted, :done] ->
        init_stop(:ignore, :normal, starter, name)
      {:suspended, nil, cont} ->
        :proc_lib.init_ack(starter, {:ok, self()})
        enter_loop(parent, dbg, name, cont)
      other ->
        reason = {:bad_return_value, other}
        init_error(reason, starter, name)
    end
  end

  # :sys callbacks

  @doc false
  def system_continue(parent, dbg, [name, cont, dest, demand]) do
    loop(parent, dbg, name, cont, dest, demand)
  end

  @doc false
  def system_terminate(reason, _, dbg, [name, cont | _]) do
    terminate(reason, dbg, name, cont)
  end

  @doc false
  def system_code_change(val, _, _, _), do: {:ok, val}

  @doc false
  def system_get_state(state), do: {:ok, state}

  @doc false
  def system_replace_state(replace, [name, cont, dest, demand]) do
    case replace.(cont) do
      cont when is_function(cont, 1) ->
        {:ok, cont, [name, cont, dest, demand]}
     end
  end

  @doc false
  def format_status(:normal, [_, sys_state, parent, dbg, [name, cont, dest, demand]]) do
    header = :gen.format_status_header('Status for StreamRunner', name)
    log = :sys.get_debug(:log, dbg, [])
    [{:header, header},
     {:data, [{'Status', sys_state},
              {'Parent', parent},
              {'Logged Events', log},
              {'Continuation', cont}]}]
  end

  ## Internal

  defp start(stream, opts, link) do
    case Keyword.pop(opts, :name) do
      {nil, opts} ->
        :gen.start(__MODULE__, link, __MODULE__, stream, opts)
      {atom, opts} when is_atom(atom) ->
        :gen.start(__MODULE__, link, {:local, atom}, __MODULE__, stream, opts)
      {{:global, _} = name, opts} ->
        :gen.start(__MODULE__, link, name, __MODULE__, stream, opts)
      {{:via, _, _} = name, opts} ->
        :gen.start(__MODULE__, link, name, __MODULE__, stream, opts)
    end
  end

  defp init_error(reason, starter, name) do
    init_stop({:error, reason}, reason, starter, name)
  end

  defp init_stop(ack, reason, starter, name) do
    _ = unregister(name)
    :proc_lib.init_ack(starter, ack)
    exit(reason)
  end

  defp unregister(pid) when is_pid(pid), do: :ok
  defp unregister({:local, name}) when is_atom(name), do: Process.unregister(name)
  defp unregister({:global, name}), do: :global.unregister_name(name)
  defp unregister({:via, mod, name}), do: apply(mod, :unregister_name, [name])

  defp enter_loop(parent, dbg, {:local, name}, cont) do
    loop(parent, dbg, name, cont, %{}, 0)
  end
  defp enter_loop(parent, dbg, name, cont) do
    loop(parent, dbg, name, cont, %{}, 0)
  end

  ## Begin different loop
  #######################

  defp loop(parent, dbg, name, cont, sinks, ag_demand) do
    # If nothing has asked for events yet, wait until something does.
    # Otherwise, immediately supply the demand
    timeout = if ag_demand == 0, do: :infinity, else: 0

    receive do
      # OTP Boilerplate
      {:EXIT, ^parent, reason} ->
        terminate(reason, dbg, name, cont)
      {:system, from, msg} ->
        :sys.handle_system_msg(msg, from, parent, __MODULE__, dbg, [name, cont, sinks, ag_demand])
      # One of the monitored subscribers exited,
      # remove all demand
      {:EXIT, pid, _} ->
        {sinks, ag_demand} = handle_demand(sinks, pid, :cancel)
        loop(parent, dbg, name, cont, sinks, ag_demand)
      # Async demand
      {:"$gen_ask", {pid, nil}, {count, _}} ->
        {sinks, ag_demand} = handle_demand(sinks, pid, count)
        loop(parent, dbg, name, cont, sinks, ag_demand)
      # Sync demand Don't use at the moment
      {:"$gen_ask", {pid, ref}, {count, _}} ->
        {sinks, ag_demand} = handle_demand(sinks, pid, count)
        send(pid, {ref, :ok})
        loop(parent, dbg, name, cont, sinks, ag_demand)
    after
      timeout ->
        try do
          cont.({:cont, nil})
        catch
          :error, value ->
            reason = {value, System.stacktrace()}
            log_stop(reason, reason, dbg, name, cont)
          :throw, value ->
            reason = {{:nocatch, value}, System.stacktrace()}
            log_stop(reason, reason, dbg, name, cont)
          :exit, value ->
            log_stop({value, System.stacktrace()}, value, dbg, name, cont)
        else
          {:suspended, _, cont} when dbg == [] ->
            {sinks, ag_demand} = supply_demand(sinks, 1)
            loop(parent, dbg, name, cont, sinks, ag_demand)
          {:suspended, _, cont} = event ->
            dbg = :sys.handle_debug(dbg, &print_event/3, name, event)
            {sinks, ag_demand} = supply_demand(sinks, 1)
            loop(parent, dbg, name, cont, sinks, ag_demand)
          {res, nil} when res in [:halted, :done] ->
            exit(:normal)
          other ->
            reason = {:bad_return_value, other}
            terminate(reason, dbg, name, cont)
        end
    end
  end

  ## StreamRouter Helpers
  #######################

  # TODO: store demand in an ordered structure
  # Most of the time we're just concerned with the minimum demand anyway
  @doc false
  def handle_demand(sinks, pid, :cancel) do
    sinks = case Map.fetch(pid) do
      {:ok, _} ->
        Process.demonitor(pid, [:flush])
        Map.delete(sinks, pid)
      :error -> sinks
    end
    {sinks, determine_demand(sinks)}
  end

  @doc false
  def handle_demand(sinks, pid, count) do
    _ = Process.monitor(pid)
    sinks = Map.put(sinks, pid, count)
    {sinks, determine_demand(sinks)}
  end

  @doc false
  def supply_demand(sinks, count) do
    sinks = sinks |> Enum.into(%{}, fn {k, v} -> {k, v - count} end)
    {sinks, determine_demand(sinks)}
  end

  @doc false
  def determine_demand(sinks) do
    {_, demand} = Enum.min_by(sinks, &elem(&1, 1))
    demand
  end

  ## Common with StreamRunner
  ## TODO: Refactor common parts
  ##############################

  defp print_event(device, {:suspended, v, cont}, name) do
    msg = ["*DBG* ", inspect(name), " got value ", inspect(v),
           ", new continuation: " | inspect(cont)]
    IO.puts(device, msg)
  end

  defp terminate(reason, dbg, name, cont) do
    try do
      cont.({:halt, nil})
    catch
      :error, value ->
        reason = {value, System.stacktrace()}
        log_stop(reason, reason, dbg, name, cont)
      :throw, value ->
        reason = {{:nocatch, value}, System.stacktrace()}
        log_stop(reason, reason, dbg, name, cont)
      :exit, value ->
        log_stop({value, System.stacktrace()}, value, dbg, name, cont)
    else
      {res, nil} when res in [:halted, :done] ->
        stop(reason, dbg, name, cont)
      other ->
        reason = {:bad_return_value, other}
        log_stop(reason, reason, dbg, name, cont)
    end
  end

  defp stop(:normal, _, _, _),                   do: exit(:normal)
  defp stop(:shutdown, _, _, _),                 do: exit(:shutdown)
  defp stop({:shutdown, _} = shutdown, _, _, _), do: exit(shutdown)
  defp stop(reason, dbg, name, cont),            do: log_stop(reason, reason, dbg, name, cont)

  defp log_stop(report_reason, reason, dbg, name, cont) do
    :error_logger.format(
      '** StreamRunner ~p terminating~n' ++
      '** When continuation      == ~p~n' ++
      '** Reason for termination == ~n' ++
      '** ~p~n', [name, cont, format_reason(report_reason)])
    :sys.print_log(dbg)
    exit(reason)
  end

  defp format_reason({:undef, [{mod, fun, args, _} | _] = stacktrace} = reason)
  when is_atom(mod) and is_atom(fun) do
    cond do
      :code.is_loaded(mod) === false ->
        {:"module could not be loaded", stacktrace}
      is_list(args) and not function_exported?(mod, fun, length(args)) ->
        {:"function not exported", stacktrace}
      is_integer(args) and not function_exported?(mod, fun, args) ->
        {:"function not exported", stacktrace}
      true ->
        reason
    end
  end
  defp format_reason(reason) do
    reason
  end
end
