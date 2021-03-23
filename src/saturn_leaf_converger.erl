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
                leaders,
                batches,
                myid}).
               
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)). 

start_link(MyId) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId], []).

handle(MyId, Message) ->
    %?LOG_INFO("New message: ~p.", [Message]),
    gen_server:cast({global, reg_name(MyId)}, Message).

clean_state(MyId) ->
    gen_server:call({global, reg_name(MyId)}, clean_state, infinity).

init([MyId]) ->
    %Name = list_to_atom(integer_to_list(MyId) ++ "converger_queue"),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    {Leaders1, Batches1} = lists:foldl(fun(PrefList, {Leaders0, Batches0}) ->
                                            {_Partition, Node} = hd(PrefList),
                                            case dict:is_key(Node, Batches0) of
                                                true ->
                                                    {Leaders0, Batches0};
                                                false ->
                                                    {[hd(PrefList) | Leaders0], dict:store(Node, [], Batches0)}
                                            end
                                           end, {[], dict:new()}, GrossPrefLists),
    erlang:send_after(?EUNOMIA_FREQ, self(), notify),
    {ok, #state{pending_timestamps=[],
                stable=0,
                old_stable=0,
                remote_clock=0,
                leaders=Leaders1,
                batches=Batches1,
                myid=MyId}}.

handle_call(clean_state, _From, S0) ->
    {reply, ok, S0}.

handle_cast({completed, TimeStamps0}, S0=#state{stable=Stable0, pending_timestamps=PendingTS0}) ->
    {TimeStamps1, Stable1} = update_stable(TimeStamps0, Stable0),
    PendingTS1 = lists:merge(PendingTS0, TimeStamps1),
    {PendingTS2, Stable2} = update_stable(PendingTS1, Stable1),
    {noreply, S0#state{stable=Stable2, pending_timestamps=PendingTS2}};

handle_cast({new_stream, Stream, _SenderId}, S0=#state{remote_clock=RClock0, batches=Batches0}) ->
    %?LOG_INFO("New stream: ~p.", [Stream]),
    {RClock2, Batches2} = lists:foldl(fun(Label, {RClock1, Batches1}) ->
                                        batch_label(Label, RClock1, Batches1)
                                      end, {RClock0, Batches0}, Stream),
    {noreply, S0#state{remote_clock=RClock2, batches=Batches2}};

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(notify, S0=#state{myid=_MyId, old_stable=OldStable, stable=Stable, leaders=Leaders, batches=Batches}) ->
    %?LOG_INFO("Compute stable. New stable ~p, old ~p.", [Stable, OldStable0]),
    erlang:send_after(?EUNOMIA_FREQ, self(), notify),
    case OldStable < Stable of
        true ->
            Clock = Stable;
        false ->
            Clock = same
    end,
    Batches2 = lists:foldl(fun({_Partition, Node}=IndexNode, Batches1) ->
                            case dict:fetch(Node, Batches1) of
                                [] ->
                                    case Clock of
                                        same ->
                                            noop;
                                        _ ->
                                            saturn_proxy_vnode:leader_batches(IndexNode, Clock, [])
                                    end,
                                    Batches1;
                                BatchNode ->
                                    saturn_proxy_vnode:leader_batches(IndexNode, Clock, BatchNode),
                                    dict:store(Node, [], Batches1)
                            end
                           end, Batches, Leaders),
    {noreply, S0#state{old_stable=Stable, batches=Batches2}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

batch_label(Label, RClock, Batches0) ->
    case Label#label.operation of
        remote_reply ->
            Payload = Label#label.payload,
            Client = Payload#payload_reply.client,
            Value = Payload#payload_reply.value,
            case Payload#payload_reply.type_call of
                sync ->
                    %noop;
                    riak_core_vnode:reply(Client, {ok, {Value, 0}});
                async ->
                    gen_server:reply(Client, {ok, {Value, 0}, RClock})
                    %noop
            end,
            {RClock, Batches0};
        remote_read ->
            BKey = Label#label.bkey,
            DocIdx = riak_core_util:chash_key(BKey),
            PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
            [{IndexNode, _Type}] = PrefList,
            {Partition, Node} = IndexNode,
            Batches1 = dict:append(Node, {Partition, Label, RClock}, Batches0),
            {RClock, Batches1};
        update ->
            BKey = Label#label.bkey,
            DocIdx = riak_core_util:chash_key(BKey),
            PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
            [{IndexNode, _Type}] = PrefList,
            {Partition, Node} = IndexNode,
            Batches1 = dict:append(Node, {Partition, Label, RClock+1}, Batches0),
            %?LOG_INFO("Converger ~p, Bkey ~p, RClock ~p", [MyId, BKey, RClock+1]),
            {RClock+1, Batches1}
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
