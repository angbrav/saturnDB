%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

%% @doc saturn_SUITE:
%%    Test the basic api of saturn
-module(saturn_SUITE).

%% common_test callbacks
-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

%% tests
-export([
         replication/1,
         remote_read/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) ->
    test_utils:init_multi_dc(?MODULE, Config).


end_per_suite(Config) ->
    Config.


init_per_testcase(_Name, Config) ->
    Config.


end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.


all() ->
    [replication,
    remote_read 
    ].

server_name(Node)->
    {saturn_client_receiver, Node}.

eventual_read(Key, RClock, Node, ExpectedResult) ->
    eventual_read(Key, Node, ExpectedResult, 0, RClock).

eventual_read(Key, Node, ExpectedResult, LClock, RClock) ->
    Result = gen_server:call(server_name(Node), {read, Key, LClock, RClock}, infinity),
    case Result of
        {ok, {ExpectedResult, _Clock}, RClock1} -> Result;
        {ok, {_, _}, RClock1} ->
            ct:print("I read: ~p, expecting: ~p",[Result, ExpectedResult]),
            timer:sleep(500),
            eventual_read(Key, RClock1, Node, ExpectedResult)
    end.

replication(Config) ->
    ct:print("Starting test replication", []),
    
    BKey={2, key1},
    [Leaf1 | Leaf2] = proplists:get_value(leafs, Config),

    %% Reading a key thats empty
    Result1 = gen_server:call(server_name(Leaf1), {read, BKey, 0, 0}, infinity),
    ?assertMatch({ok, {empty, 0}, 0}, Result1),

    %% Update key
    Result2 = gen_server:call(server_name(Leaf1), {update, BKey, 3, 0}, infinity),
    ?assertMatch({ok, _Clock1}, Result2),

    %timer:sleep(10000),

    Result3 = gen_server:call(server_name(Leaf1), {read, BKey, 0, 0}, infinity),
    ?assertMatch({ok, {3, _Clock1}, 0}, Result3),

    Result4 = eventual_read(BKey, 0, Leaf2, 3),
    ?assertMatch({ok, {3, _Clock1}, 1}, Result4).

remote_read(Config) ->
    ct:print("Starting test remote_read", []),
   
    BKey={0, key1},
    [Leaf1 | Leaf2] = proplists:get_value(leafs, Config),

    %% Reading a key thats empty
    Result1 = gen_server:call(server_name(Leaf1), {read, BKey, 0, 0}, infinity),
    ?assertMatch({ok, {empty, 0}, 0}, Result1),

    %% Update key
    Result2 = gen_server:call(server_name(Leaf1), {update, BKey, 3, 0}, infinity),
    ?assertMatch({ok, _Clock1}, Result2),

    Result3 = gen_server:call(server_name(Leaf1), {read, BKey, 0, 0}, infinity),
    ?assertMatch({ok, {3, _Clock1}, 0}, Result3),

    ct:print("About to perform the remote read", []),

    Result4 = eventual_read(BKey, 0, Leaf2, 3),
    ?assertMatch({ok, {3, _Clock1}, 1}, Result4).
