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
