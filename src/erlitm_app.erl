-module(erlitm_app).
-behaviour(application).

%% API
-export([start/0, stop/0, restart/0, application/0]).

%% application callbacks
-export([start/2, stop/1]).

-define(APP, erlitm).

%%
%% API
%%

-spec start() -> {ok, Started :: [App :: atom()]} | {error, any()}.
start() ->
    application:ensure_all_started(?APP).

-spec stop() -> ok | {error, any()}.
stop() ->
    application:stop(?APP).

-spec restart() -> {ok, Started :: [App :: atom()]} | {error, any()}.
restart() ->
    stop(),
    start().

-spec application() -> atom().
application() ->
    ?APP.

%%
%% application callbacks
%%

-spec start(StartType, StartArgs :: any()) -> {ok, pid()} | {ok, pid(), State :: any()} | {error, any()} when
      StartType :: application:start_type().
start(_StartType, _StartArgs) ->
    erlitm_sup:start_link().

-spec stop(State :: any()) -> any().
stop(_State) ->
    ok.
