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
