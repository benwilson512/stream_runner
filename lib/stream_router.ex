defmodule StreamRouter do
  @moduledoc """
  The `StreamRunner` provides a convenient way to run a `Stream` as process in a
  supervisor tree.

  To run a `Stream` as a process simply pass the stream to `StreamRunner.start_link/1`:

      stream = Stream.interval(1_000) |> Stream.each(&IO.inspect/1)
      {:ok, pid} = StreamRunner.start_link(stream)

  """

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
  def start_link(stream, dest, opts \\ []) do
    start(stream, dest, opts, :link)
  end

  @doc """
  Start a `StreamRunner`.

  The `StreamRunner` is not linked to the calling process.
  """
  def start(stream, dest, opts \\ []) do
    start(stream, dest, opts, :nolink)
  end

  ## :gen callbacks

  @doc false
  def init_it(starter, :self, name, mod, args, opts) do
    init_it(starter, self(), name, mod, args, opts)
  end
  def init_it(starter, parent, name, __MODULE__, {stream, dest}, opts) do
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
        enter_loop(parent, dbg, name, cont, dest)
      other ->
        reason = {:bad_return_value, other}
        init_error(reason, starter, name)
    end
  end

  ## :sys callbacks

  @doc false
  def system_continue(parent, dbg, [name, cont, dest, demand]) do
    loop(parent, dbg, name, cont, dest, demand)
  end

  @doc false
  def system_terminate(reason, _, dbg, [name, cont]) do
    terminate(reason, dbg, name, cont)
  end

  @doc false
  def system_code_change([name, cont], _, _, _), do: {:ok, [name, cont]}

  @doc false
  def system_get_state([_, cont]), do: {:ok, cont}

  @doc false
  def system_replace_state(replace, [name, cont]) do
    case replace.(cont) do
      cont when is_function(cont, 1) ->
        {:ok, cont, [name, cont]}
     end
  end

  @doc false
  def format_status(:normal, [_, sys_state, parent, dbg, [name, cont]]) do
    header = :gen.format_status_header('Status for StreamRunner', name)
    log = :sys.get_debug(:log, dbg, [])
    [{:header, header},
     {:data, [{'Status', sys_state},
              {'Parent', parent},
              {'Logged Events', log},
              {'Continuation', cont}]}]
  end

  ## Internal

  defp start(stream, dest, opts, link) do
    case Keyword.pop(opts, :name) do
      {nil, opts} ->
        :gen.start(__MODULE__, link, __MODULE__, {stream, dest}, opts)
      {atom, opts} when is_atom(atom) ->
        :gen.start(__MODULE__, link, {:local, atom}, __MODULE__, {stream, dest}, opts)
      {{:global, _} = name, opts} ->
        :gen.start(__MODULE__, link, name, __MODULE__, {stream, dest}, opts)
      {{:via, _, _} = name, opts} ->
        :gen.start(__MODULE__, link, name, __MODULE__, {stream, dest}, opts)
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

  defp enter_loop(parent, dbg, {:local, name}, cont, dest) do
    loop(parent, dbg, name, cont, dest, 0)
  end
  defp enter_loop(parent, dbg, name, cont, dest) do
    loop(parent, dbg, name, cont, dest, 0)
  end

  # If there's no demand, simply wait for a message
  defp loop(parent, dbg, name, cont, dest, 0 = demand) do
    receive do
      {:EXIT, ^parent, reason} ->
        terminate(reason, dbg, name, cont)
      {:system, from, msg} ->
        :sys.handle_system_msg(msg, from, parent, __MODULE__, dbg, [name, cont, dest, demand])
      {:"$gen_ask", {^dest, _}, {count, _}} ->
        loop(parent, dbg, name, cont, dest, count)
    end
  end

  # Even if there is demand we want to check the inbox
  # on each iteration to ensure that it is cleared as often
  # as necessary.
  defp loop(parent, dbg, name, cont, dest, demand) do
    receive do
      {:EXIT, ^parent, reason} ->
        terminate(reason, dbg, name, cont)
      {:system, from, msg} ->
        :sys.handle_system_msg(msg, from, parent, __MODULE__, dbg, [name, cont, dest, demand])
      {:"$gen_ask", {^dest, _}, {count, _}} ->
        loop(parent, dbg, name, cont, dest, demand + count)
    after
      0 ->
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
          {:suspended, val, cont} when dbg == [] ->
            send(dest, {:"$gen_route", {dest, nil}, [val]})
            loop(parent, dbg, name, cont, dest, demand - 1)
          {:suspended, val, cont} = event ->
            dbg = :sys.handle_debug(dbg, &print_event/3, name, event)
            send(dest, {:"$gen_route", {dest, nil}, [val]})
            loop(parent, dbg, name, cont, dest, demand - 1)
          {res, nil} when res in [:halted, :done] ->
            send(dest, {:"$gen_route", {dest, nil}, {:eos, res}})
            exit(:normal)
          other ->
            reason = {:bad_return_value, other}
            terminate(reason, dbg, name, cont)
        end
    end
  end

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

  defp stop(:normal, _, _, _),                  do: exit(:normal)
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
