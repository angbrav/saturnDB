%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015-2016 INESC-ID, Instituto Superior Tecnico,
%%                         Universidade de Lisboa, Portugal
%% Copyright (c) 2015-2016 Universite Catholique de Louvain, Belgium
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%  
%% -------------------------------------------------------------------
-module(saturn_leaf_app).

-behaviour(application).
-include("saturn_leaf.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case saturn_leaf_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, saturn_proxy_vnode}]),
            ok = riak_core_node_watcher:service_up(?PROXY_SERVICE, self()),

            ok = enable_debug_logs(),
        
            case ?BACKEND of
                simple_backend ->
                    ok = riak_core:register([{vnode_module, saturn_simple_backend_vnode}]),
                    ok = riak_core_node_watcher:service_up(?SIMPLE_SERVICE, self());
                _ ->
                    noop
            end,

            ok = riak_core_ring_events:add_guarded_handler(saturn_leaf_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(saturn_leaf_node_event_handler, []),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

-ifdef(debug_log).
-spec enable_debug_logs() -> ok.
enable_debug_logs() ->
    logger:add_handler(debug, logger_std_h, #{
        filters => [{debug, {fun logger_filters:level/2, {stop, neq, debug}}}],
        config => #{file => "log/debug.log"}
    }).
-else.
-spec enable_debug_logs() -> ok.
enable_debug_logs() -> ok.
-endif.

stop(_State) ->
    ok.
