# `gen_server` in Erlang/OTP: Research
> Examples compiled and executed on **Erlang/OTP 25** (the described API is stable in later versions; the documentation consulted corresponds to OTP 27 / stdlib 6.2).
> 
## 1. Purpose of gen_server: What Problem It Solves
Almost every Erlang program eventually needs a **server process**: a long-lived process that maintains **state** and handles **requests** from other processes. Writing it by hand with spawn, receive, and tail recursion forces you to solve the same problems repeatedly:
 * the receive loop and passing state from one iteration to the next;
 * the request/response protocol (references, monitor, *timeouts*);
 * process registration by name;
 * handling system messages (debugging, tracing, hot code swapping);
 * ordered startup and shutdown within a supervision tree.
gen_server is a **behaviour**: a generic module (part of stdlib) that implements all that common plumbing, while your **callback module** implements only the problem-specific logic. The documentation describes it as the server side of a client-server relationship. Separating "the generic" from "the specific" yields shorter, more uniform code that has been proven in production for decades.
## 2. Importance in OTP
| Aspect | What gen_server Provides |
|---|---|
| **Processes** | Each gen_server is a lightweight Erlang process with its own mailbox and heap; created with start_link/start. |
| **State** | State is the second argument of each callback and is returned updated on every response. There are no shared mutable variables: state is private to the process. |
| **Concurrency** | Messages arrive in the mailbox one by one and are processed **sequentially**. That eliminates race conditions on state, though it also means a slow call blocks other requests to that server. Concurrency comes from having *many* processes. |
| **Supervision** | Started with start_link, which links the process to its parent; this fits it into a supervision tree (supervisor). The OTP guide states that start_link is required when the server is part of a supervision tree, while gen_server:start is for standalone servers. |
| **Fault Tolerance** | "Let it crash" philosophy: if the server fails, the supervisor restarts it according to its strategy (one_for_one, one_for_all, rest_for_one) and max restart intensity. All processes built with standard OTP behaviours automatically follow the supervisor shutdown protocol. |
| **Debugging** | Handles system messages from the sys module (sys:get_state/1, sys:suspend/1, traces, etc.) without extra code. |
Other behaviours (gen_statem, gen_event, supervisor) follow the same idea: a generic module plus custom callbacks. gen_server is the most widely used and the foundation for understanding the rest.
## 3. How to Use It
### 3.1 Relationship Between the API and Callbacks
```text
gen_server:start_link ------> Module:init/1
gen_server:call       ------> Module:handle_call/3
gen_server:cast       ------> Module:handle_cast/2
(any message: Pid ! Msg) -> Module:handle_info/2
gen_server:stop / crash ---> Module:terminate/2

```
### 3.2 Callbacks
| Callback | When It Is Invoked | Typical Return Values |
|---|---|---|
| init(Args) | Once, on startup. start_link **does not return** until init finishes. | {ok, State}, {ok, State, Timeout}, ignore, {stop, Reason} |
| handle_call(Request, From, State) | **Synchronous** request (gen_server:call). | {reply, Reply, NewState}, {noreply, NewState} (reply later with gen_server:reply/2), {stop, Reason, Reply, NewState} |
| handle_cast(Msg, State) | **Asynchronous** request (gen_server:cast). | {noreply, NewState}, {stop, Reason, NewState} |
| handle_info(Info, State) | Any message not from call/cast: Pid ! Msg, {'EXIT', ...}, {'DOWN', ...}, *timeouts*. | {noreply, NewState}, {stop, Reason, NewState} |
| terminate(Reason, State) | Right before the process exits in a controlled way (a callback returned stop, gen_server:stop/1, or the parent died **and** the server traps exits with process_flag(trap_exit, true)). Useful for releasing resources. | Return value is ignored. |
Important notes:
 * terminate/2 **is not guaranteed** on every exit: if the process receives exit(Pid, kill), it dies without running it (seen in the supervision demo below: no terminate message appears). Therefore, do not rely on it for critical guarantees.
 * The callbacks handle_continue/2, code_change/3, and format_status exist but are optional.
 * A missing handle_info/2 causes unexpected messages to be logged as errors; it is good practice to include a catch-all clause.
### 3.3 start_link
```erlang
gen_server:start_link({local, Name}, Module, Args, Options) -> {ok, Pid} | ignore | {error, Reason}
gen_server:start_link(Module, Args, Options)                %% unregistered

```
 * Creates the process, **links** it to the caller, and is **synchronous** (waits for init/1).
 * {local, Name} registers the process with register/2; {global, N} and {via, Mod, N} also exist.
 * Args is passed directly to init/1.
 * Convention: wrap it in a module API function (counter:start_link/1) so clients do not need to know behaviour details.
### 3.4 call vs cast
|  | gen_server:call(Ref, Msg [, Timeout]) | gen_server:cast(Ref, Msg) |
|---|---|---|
| Nature | **Synchronous**: blocks the client until a reply arrives | **Asynchronous**: returns ok immediately |
| Callback | handle_call/3 | handle_cast/2 |
| Reply | Yes ({reply, ...}) | No |
| Timeout | 5000 ms by default; if exceeded, the client receives an exit with reason timeout | Not applicable |
| If server does not exist | Client fails with exit (noproc) | cast still returns ok: **ignores** that the target does not exist |
| Backpressure | Natural: the client waits, so it does not flood the server | None: a fast client can fill the mailbox |
| Use it for | Queries and operations whose result/confirmation you need | "Fire-and-forget" notifications |
Rule of thumb: when in doubt, **prefer call**, because it provides confirmation and backpressure. Use cast when you truly do not care about the result.
## 4. Working Example
Three pieces are included: a **counter** (counter), a **stack** (stack), and a **supervisor** (sup) that restarts the counter when it dies.
### 4.1 counter.erl
```erlang
-module(counter).
-behaviour(gen_server).

%% API
-export([start_link/1, increment/1, decrement/1, get/1, reset/1, stop/1]).
%% Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%%% ---------- API (client) ----------
start_link(Initial) when is_integer(Initial) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Initial, []).

increment(Pid) -> gen_server:cast(Pid, increment).   %% asynchronous
decrement(Pid) -> gen_server:cast(Pid, decrement).   %% asynchronous
reset(Pid)     -> gen_server:cast(Pid, reset).       %% asynchronous
get(Pid)       -> gen_server:call(Pid, get).         %% synchronous
stop(Pid)      -> gen_server:stop(Pid).              %% exits with reason normal

%%% ---------- Callbacks (server) ----------
init(Initial) ->
    io:format("[counter] init with value ~p~n", [Initial]),
    process_flag(trap_exit, true),   %% so terminate/2 is called if the parent dies
    {ok, Initial}.

handle_call(get, _From, State) ->
    {reply, State, State};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(increment, State) -> {noreply, State + 1};
handle_cast(decrement, State) -> {noreply, State - 1};
handle_cast(reset, _State)    -> {noreply, 0};
handle_cast(_Msg, State)      -> {noreply, State}.

handle_info(ping, State) ->
    io:format("[counter] received ping (raw message), state=~p~n", [State]),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State) ->
    io:format("[counter] terminate: reason=~p, final state=~p~n", [Reason, State]),
    ok.

```
### 4.2 stack.erl
```erlang
-module(stack).
-behaviour(gen_server).
-export([start_link/0, push/2, pop/1, size/1]).
-export([init/1, handle_call/3, handle_cast/2]).

start_link() -> gen_server:start_link(?MODULE, [], []).
push(Pid, X) -> gen_server:cast(Pid, {push, X}).
pop(Pid)     -> gen_server:call(Pid, pop).
size(Pid)    -> gen_server:call(Pid, size).

init([]) -> {ok, []}.

handle_call(pop, _From, [])      -> {reply, {error, empty}, []};
handle_call(pop, _From, [H|T])   -> {reply, {ok, H}, T};
handle_call(size, _From, S)      -> {reply, length(S), S}.

handle_cast({push, X}, S) -> {noreply, [X|S]}.

```
### 4.3 sup.erl (supervisor)
```erlang
-module(sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).

start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 3, period => 10},
    Child = #{id => counter,
              start => {counter, start_link, [100]},
              restart => permanent, shutdown => 5000, type => worker},
    {ok, {SupFlags, [Child]}}.

```
### 4.4 demo.escript (test script)
```erlang
#!/usr/bin/env escript
%%! -pa .
main(_) ->
    io:format("== 1) counter ==~n"),
    {ok, C} = counter:start_link(0),
    counter:increment(C), counter:increment(C), counter:increment(C), counter:decrement(C),
    io:format("get -> ~p~n", [counter:get(C)]),
    C ! ping,
    counter:reset(C),
    io:format("after reset -> ~p~n", [counter:get(C)]),
    io:format("unknown call -> ~p~n", [gen_server:call(C, foo)]),
    counter:stop(C),
    io:format("~n== 2) stack ==~n"),
    {ok, S} = stack:start_link(),
    stack:push(S, a), stack:push(S, b), stack:push(S, c),
    io:format("size -> ~p~n", [stack:size(S)]),
    io:format("pop -> ~p~n", [stack:pop(S)]),
    io:format("pop -> ~p~n", [stack:pop(S)]),
    io:format("pop -> ~p~n", [stack:pop(S)]),
    io:format("pop -> ~p~n", [stack:pop(S)]),
    io:format("~n== 3) supervision ==~n"),
    unlink(S),
    {ok, Sup} = sup:start_link(),
    unlink(Sup),
    Old = whereis(counter),
    counter:increment(Old),
    io:format("state before crash: ~p (pid ~p)~n", [counter:get(Old), Old]),
    exit(Old, kill),
    timer:sleep(200),
    New = whereis(counter),
    io:format("pid after restart: ~p (different: ~p)~n", [New, New =/= Old]),
    io:format("state after restart: ~p~n", [counter:get(New)]),
    io:format("~n== 4) call timeout ==~n"),
    {ok, P} = gen_server:start_link(counter, 0, []),
    unlink(P),
    sys:suspend(P),
    R = (catch gen_server:call(P, get, 300)),
    {'EXIT', {Reason, _}} = R, io:format("call to suspended process -> exit with reason ~p~n", [Reason]).

```
### 4.5 Compile and Run
```bash
erlc counter.erl stack.erl sup.erl
escript demo.escript

```
### 4.6 Actual Execution Output
```text
== 1) counter ==
[counter] init with value 0
get -> 2
[counter] received ping (raw message), state=2
after reset -> 0
unknown call -> {error,unknown_call}
[counter] terminate: reason=normal, final state=0

== 2) stack ==
size -> 3
pop -> {ok,c}
pop -> {ok,b}
pop -> {ok,a}
pop -> {error,empty}

== 3) supervision ==
[counter] init with value 100
state before crash: 101 (pid <0.82.0>)
[counter] init with value 100
=SUPERVISOR REPORT==== 15-Oct-2026::22:45:50.528156 ===
    supervisor: {local,sup}
    errorContext: child_terminated
    reason: killed
    offender: [{pid,<0.82.0>},
               {id,counter},
               {mfargs,{counter,start_link,"d"}},
               {restart_type,permanent},
               {significant,false},
               {shutdown,5000},
               {child_type,worker}]

pid after restart: <0.83.0> (different: true)
state after restart: 100

== 4) call timeout ==
[counter] init with value 0
call to suspended process -> exit with reason timeout

```
### 4.7 What the Output Demonstrates
 1. **Counter**: three increment calls and one decrement call (asynchronous) leave the state at 2. The synchronous get confirms it. Because messages from the same sender arrive in order, get sees the effect of all prior cast messages.
 2. **handle_info/2**: C ! ping does not go through the gen_server API, so handle_info handles it.
 3. **Unknown call**: the catch-all clause returns {error, unknown_call} instead of leaving the client hanging until *timeout*.
 4. **terminate/2**: calling counter:stop/1 prints the reason normal and the final state.
 5. **Stack**: push is a cast, pop/size are calls; pop on an empty stack returns {error, empty}.
 6. **Supervision**: the process is killed with exit(Pid, kill). The supervisor detects it, emits the *SUPERVISOR REPORT*, and starts a **new** process (different PID) with init/1 again: the state returns to the initial value (100), not the previous 101. Keep this in mind: **restarting does not recover state**; if you need it, persist it outside the process (ETS, disk, database). terminate/2 was also not printed, because kill cannot be trapped.
 7. **Call timeout**: against a process suspended with sys:suspend/1, the client exits with reason timeout.
> Reading note: the *report* shows {counter,start_link,"d"} because the argument list [100] matches the ASCII code for the letter "d" and the formatter displays it as a string.
> 
## 5. Summary of Best Practices
 * Keep callbacks **fast**; delegate long work to another process (or use handle_continue/2, or reply later with gen_server:reply/2).
 * Expose a **function API** and hide internal messages.
 * Include a catch-all clause in handle_call, handle_cast, and handle_info.
 * Use start_link under a supervisor; choose a strategy and intensity/period suited to the use case.
 * Do not rely on terminate/2 for critical guarantees.
 * Use -behaviour(gen_server). so the compiler warns about missing callbacks.
## 6. References Consulted
 1. Erlang/OTP, **gen_server (stdlib)**, module documentation: https://erlang.org/docs/27/apps/stdlib/gen_server
 2. Erlang/OTP, **OTP Design Principles: gen_server Behaviour**: https://erlang.org/doc/design_principles/gen_server_concepts
 3. Erlang/OTP, **OTP Design Principles: Supervisor Behaviour**: https://www.erlang.org/docs/21/design_principles/sup_princ.html (and the OTP 18 edition: https://erlang.org/docs/18/design_principles/sup_princ)
 4. Erlang/OTP, **supervisor (stdlib)**, restart strategies and child specification: https://erlang.org/docs/20/man/supervisor
 5. Own execution of the examples with erlc/escript on Erlang/OTP 25.
