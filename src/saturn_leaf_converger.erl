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
-module(saturn_leaf_converger).
-behaviour(gen_server).

-include("saturn_leaf.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([handle/2,
         clean_state/1]).

-record(state, {pending_timestamps,
                stable,
                old_stable,
                remote_clock,
                partitions,
                myid}).
               
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)). 

start_link(MyId) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId], []).

handle(MyId, Message) ->
    ?LOG_INFO("New message: ~p.", [Message]),
    gen_server:cast({global, reg_name(MyId)}, Message).

clean_state(MyId) ->
    gen_server:call({global, reg_name(MyId)}, clean_state, infinity).

init([MyId]) ->
    %Name = list_to_atom(integer_to_list(MyId) ++ "converger_queue"),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    Dict = lists:foldl(fun(PrefList, Acc) ->
                        dict:store(hd(PrefList), 0, Acc)
                       end, dict:new(), GrossPrefLists),
    erlang:send_after(10, self(), notify),
    {ok, #state{pending_timestamps=[],
                stable=0,
                old_stable=0,
                remote_clock=0,
                partitions=dict:fetch_keys(Dict),
                myid=MyId}}.

handle_call(clean_state, _From, S0) ->
    {reply, ok, S0}.

handle_cast({completed, TimeStamps0}, S0=#state{stable=Stable0, pending_timestamps=PendingTS0}) ->
    {TimeStamps1, Stable1} = update_stable(TimeStamps0, Stable0),
    PendingTS1 = orddict:merge(fun(_K, V1, _V2) ->
                                    V1
                               end, PendingTS0, TimeStamps1), 
    {PendingTS2, Stable2} = update_stable(PendingTS1, Stable1),
    {noreply, S0#state{stable=Stable2, pending_timestamps=PendingTS2}};

handle_cast({new_stream, Stream, _SenderId}, S0=#state{remote_clock=RClock0}) ->
    ?LOG_INFO("New stream: ~p.", [Stream]),
    RClock2= lists:foldl(fun(Label, RClock1) ->
                            handle_label(Label, RClock1)
                         end, RClock0, Stream),
    {noreply, S0#state{remote_clock=RClock2}};

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(notify, S0=#state{myid=_MyId, old_stable=OldStable0, stable=Stable, partitions=Partitions}) ->
    %?LOG_INFO("Compute stable. New stable ~p, old ~p.", [Stable, OldStable0]),
    case OldStable0 < Stable of
        true ->
            lists:foreach(fun(Partition) ->
                            saturn_proxy_vnode:new_remote_clock(Partition, Stable)
                          end, Partitions);
        false ->
            noop
    end,
    %erlang:send_after(?STABILIZATION_FREQ, self(), notify),
    erlang:send_after(100, self(), notify),
    {noreply, S0#state{old_stable=Stable}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_label(empty, Queue) ->
    Queue;

handle_label(Label, RClock) ->
    case Label#label.operation of
        remote_read ->
            BKey = Label#label.bkey,
            DocIdx = riak_core_util:chash_key(BKey),
            PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
            [{IndexNode, _Type}] = PrefList,
            saturn_proxy_vnode:remote_read(IndexNode, Label, RClock),
            RClock;
        remote_reply ->
            Payload = Label#label.payload,
            Client = Payload#payload_reply.client,
            Value = Payload#payload_reply.value,
            case Payload#payload_reply.type_call of
                sync ->
                    %noop;
                    riak_core_vnode:reply(Client, {ok, {Value, 0}});
                async ->
                    gen_server:reply(Client, {ok, {Value, 0}})
                    %noop
            end,
            RClock;
        update ->
            BKey = Label#label.bkey,
            Clock = Label#label.timestamp,
            Node = Label#label.node,
            Sender = Label#label.sender,
            DocIdx = riak_core_util:chash_key(BKey),
            PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
            [{IndexNode, _Type}] = PrefList,
            saturn_proxy_vnode:propagate(IndexNode, Clock, Node, Sender, RClock+1),
            RClock+1
    end.

update_stable([], Stable) ->
    {[], Stable};

update_stable([First|Rest]=TimeStamps, Stable) ->
    case First==Stable+1 of
        true ->
            update_stable(Rest, First);
        false ->
            {TimeStamps, Stable}
    end.
    
-ifdef(TEST).

-endif.
