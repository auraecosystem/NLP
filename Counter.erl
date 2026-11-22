-module(counter).
-behaviour(gen_server).

%% API
-export([start_link/1, increment/1, decrement/1, get/1, reset/1, stop/1]).
%% Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%%% ---------- API (cliente) ----------
start_link(Initial) when is_integer(Initial) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Initial, []).

increment(Pid) -> gen_server:cast(Pid, increment).   %% asíncrono
decrement(Pid) -> gen_server:cast(Pid, decrement).   %% asíncrono
reset(Pid)     -> gen_server:cast(Pid, reset).       %% asíncrono
get(Pid)       -> gen_server:call(Pid, get).         %% síncrono
stop(Pid)      -> gen_server:stop(Pid).              %% termina con razon normal

%%% ---------- Callbacks (servidor) ----------
init(Initial) ->
    io:format("[counter] init con valor ~p~n", [Initial]),
    process_flag(trap_exit, true),   %% para que se invoque terminate/2 si el padre muere
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
    io:format("[counter] recibi ping (mensaje crudo), estado=~p~n", [State]),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State) ->
    io:format("[counter] terminate: razon=~p, estado final=~p~n", [Reason, State]),
    ok.
