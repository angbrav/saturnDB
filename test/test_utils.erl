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

-module(test_utils).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(FORCE_KILL_TIMER, 1500).
-define(RIAK_SLEEP, 5000).

-export([
    at_init_testsuite/0,
    pmap/2,
    bucket/1,
    init_single_dc/2,
    init_multi_dc/2,
    get_node_name/1,
    web_ports/1,
    restart_nodes/2,
    partition_cluster/2,
    heal_cluster/2,
    set_up_clusters_common/1,
    unpack/1,
    server_name/1,
    log_config/1
]).

%% ===========================================
%% Node utilities
%% ===========================================

-export([
    start_node/2,
    kill_nodes/1,
    kill_and_restart_nodes/2,
    brutal_kill_nodes/1
]).

%% Logger API
-export([log/2]).

log(#{msg := Msg}, _Config) ->
    Master = application:get_env(saturn_leaf, ct_master, undefined),
    Arg = case Msg of
        {report, Report} ->
            io_lib:format("log report: ~p", [Report]);
        {string, Str} ->
            io_lib:format("string: ~s", [Str]);
        {Format, Terms} ->
            io_lib:format(Format, Terms)
    end,
    _ = rpc:call(Master, ct, log, [Arg]).

%% ===========================================
%% Common Test Initialization
%% ===========================================

init_single_dc(Suite, Config) ->
    ct:pal("[~p]", [Suite]),
    test_utils:at_init_testsuite(),

    Config2 = [{groups_file, "config/buckets/groups_test_single.txt"} | [{tree_file, "config/trees/tree_test_single.txt"} | Config]],

    StartDCs = fun(Nodes) ->
        test_utils:pmap(fun(N) -> {_Status, Node} = test_utils:start_node(N, Config2), Node end, Nodes)
               end,
    [Nodes] = test_utils:pmap( fun(N) -> StartDCs(N) end, [[dev1]] ),
    [Node] = Nodes,

    Port = web_ports(dev1),

    {ok, _} = rpc:call(Node, saturn_leaf_sup, start_leaf, [Port+1, 0, true, true]),
    pong = net_adm:ping(Node),
    ok = wait_for_name(list_to_atom("1saturn_leaf_producer")),
    ok = rpc:call(Node, saturn_leaf_producer, check_ready, [1]),
    ok = rpc:call(Node, saturn_leaf_receiver, assign_convergers, [0, 1]),

    [{clusters, [Nodes]} | [{nodes, Nodes} | [{node, Node} | Config2]]].


init_multi_dc(Suite, Config0) ->
    ct:pal("[~p]", [Suite]),
    ct:log("Starting init_multi_dc ~p", [Suite]),

    Config = [{groups_file, "buckets/two_leafs_buckets.txt"} | [{tree_file, "trees/two_leafs_one_internal.txt"} | Config0]],

    at_init_testsuite(),

    {ready, Leaf1} = start_node(dev1, Config),
    {ready, Leaf2} = start_node(dev2, Config),
    {ready, Internal1} = start_node(dev3, Config),
    
    pong = rpc:call(Leaf1, net_adm, ping, [Leaf2]),
    pong = rpc:call(Leaf1, net_adm, ping, [Internal1]),
    pong = rpc:call(Leaf2, net_adm, ping, [Internal1]), 
    
    pong = net_adm:ping(Leaf1),
    pong = net_adm:ping(Leaf2),
    pong = net_adm:ping(Internal1),
    
    {ok, _} = rpc:call(Leaf1, saturn_leaf_sup, start_leaf, [web_ports(dev1)+1, 0, true, true]),
    ok = wait_for_name(list_to_atom("0saturn_leaf_producer")),
    ok = rpc:call(Leaf1, saturn_leaf_producer, check_ready, [0]),
    ok = wait_for_name(list_to_atom("0saturn_leaf_receiver")),
    ok = wait_for_name(list_to_atom("0saturn_leaf_converger")),
    

    {ok, _} = rpc:call(Leaf2, saturn_leaf_sup, start_leaf, [web_ports(dev2)+1, 1, true, true]),
    ok = wait_for_name(list_to_atom("1saturn_leaf_producer")),
    ok = rpc:call(Leaf2, saturn_leaf_producer, check_ready, [1]),
    ok = wait_for_name(list_to_atom("1saturn_leaf_receiver")),
    ok = wait_for_name(list_to_atom("1saturn_leaf_converger")),

    {ok, _} = rpc:call(Internal1, saturn_leaf_sup, start_internal, [web_ports(dev3)+1, 2]),
    ok = wait_for_name(list_to_atom("2saturn_internal_serv")),

    ok = rpc:call(Leaf1, saturn_leaf_receiver, assign_convergers, [0, 2]),
    ok = rpc:call(Leaf2, saturn_leaf_receiver, assign_convergers, [1, 2]),

    ok = wait_until_ring_ready(Leaf1),
    ok = wait_until_ring_ready(Leaf2),

    ok = rpc:call(Leaf1, saturn_leaf, start_dissemination, []),
    ok = rpc:call(Leaf2, saturn_leaf, start_dissemination, []),

    %DocIdx = rpc:call(Leaf1, riak_core_util, chash_key, [{1, key1}]),
    %IndexNode1 = rpc:call(Leaf1, riak_core_apl, get_primary_apl, [DocIdx, 1, saturn_proxy]),
    %IndexNode2 = rpc:call(Leaf2, riak_core_apl, get_primary_apl, [DocIdx, 1, saturn_proxy]),

    %ct:print("indexnode1: ~p, indexnode2: ~p", [IndexNode1, IndexNode2]),

    [{leafs, [Leaf1|Leaf2]} | Config].

at_init_testsuite() ->
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@" ++ Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        {error, {{already_started, _}, _}} -> ok
    end.


%% ===========================================
%% Node utilities
%% ===========================================

start_node(Name, Config) ->
    %% code path for compiled dependencies (ebin folders)
    CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()),
    ct:log("Starting node ~p", [Name]),

    {ok, Cwd} = file:get_cwd(),
    SaturnFolder = filename:dirname(filename:dirname(Cwd)),
    PrivDir = proplists:get_value(priv_dir, Config),
    DataTestDir = proplists:get_value(data_dir, Config),
    NodeDir = filename:join([PrivDir, Name]) ++ "/",
    filelib:ensure_dir(NodeDir),
    ct:log("Saturn folder ~p", [SaturnFolder]),

    %% have the slave nodes monitor the runner node, so they can't outlive it
    NodeConfig = [
        %% have the slave nodes monitor the runner node, so they can't outlive it
        {monitor_master, true},

        %% set code path for dependencies
        {startup_functions, [ {code, set_path, [CodePath]} ]}],
    case ct_slave:start(Name, NodeConfig) of
        {ok, Node} ->
            % load application to allow for configuring the environment before starting
            ok = rpc:call(Node, application, load, [riak_core]),
            %ok = rpc:call(Node, application, load, [antidote_stats]),
            %ok = rpc:call(Node, application, load, [ranch]),
            ok = rpc:call(Node, application, load, [saturn_leaf]),

            %% get remote working dir of node
            {ok, NodeWorkingDir} = rpc:call(Node, file, get_cwd, []),

            %% DATA DIRS
            ok = rpc:call(Node, application, set_env, [saturn_leaf, data_dir, filename:join([NodeWorkingDir, Node, "saturn-data"])]),
            ok = rpc:call(Node, application, set_env, [riak_core, ring_state_dir, filename:join([NodeWorkingDir, Node, "data"])]),
            ok = rpc:call(Node, application, set_env, [riak_core, platform_data_dir, filename:join([NodeWorkingDir, Node, "data"])]),
            %ok = rpc:call(Node, application, set_env, [riak_core, schema_dirs, [AntidoteFolder ++ "/_build/default/rel/antidote/lib/"]]),


            %% PORTS
            Port = web_ports(Name),
            %ok = rpc:call(Node, application, set_env, [antidote, logreader_port, Port]),
            %ok = rpc:call(Node, application, set_env, [antidote, pubsub_port, Port + 1]),
            %ok = rpc:call(Node, application, set_env, [ranch, pb_port, Port + 2]),
            ok = rpc:call(Node, application, set_env, [riak_core, handoff_port, Port]),
            %ok = rpc:call(Node, application, set_env, [antidote_stats, metrics_port, Port + 4]),


            %% LOGGING Configuration
            %% add additional logging handlers to ensure easy access to remote node logs
            %% for each logging level
            %LogRoot = filename:join([NodeWorkingDir, Node, "logs"]),
            %% set the logger configuration
            %ok = rpc:call(Node, application, set_env, [saturn_leaf, logger, log_config(LogRoot)]),
            %% set primary output level, no filter
            %rpc:call(Node, logger, set_primary_config, [level, all]),
            %% load additional logger handlers at remote node
            %rpc:call(Node, logger, add_handlers, [saturn_leaf]),



            ok = rpc:call(Node, logger, set_primary_config, [level, all]),
            ok = rpc:call(Node, application, set_env, [saturn_leaf, ct_master, node()]),
            ConfLog = #{level => info,
                        formatter => {logger_formatter, #{single_line => true,
                                                          max_size => 2048}},
                        config => #{type => standard_io}},

            ok = rpc:call(Node, logger, add_handler, [saturn_leaf_ct_redirect, ?MODULE, ConfLog]),


            %% redirect slave logs to ct_master logs
            %ok = rpc:call(Node, application, set_env, [saturn_leaf, ct_master, node()]),
            %ConfLog = #{level => debug, formatter => {logger_formatter, #{single_line => true, max_size => 2048}}, config => #{type => standard_io}},
            %_ = rpc:call(Node, logger, add_handler, [saturn_leaf_redirect_ct, ct_redirect_handler, ConfLog]),


            %% ANTIDOTE Configuration
            TreeFile = proplists:get_value(tree_file, Config),
            GroupsFile = proplists:get_value(groups_file, Config),

            ok = rpc:call(Node, application, set_env, [saturn_leaf, tree, DataTestDir ++ TreeFile]),
            ok = rpc:call(Node, application, set_env, [saturn_leaf, groups, DataTestDir ++ GroupsFile]),
            %% reduce number of actual log files created to 4, reduces start-up time of node
            ok = rpc:call(Node, application, set_env, [riak_core, ring_creation_size, 4]),
            %ok = rpc:call(Node, application, set_env, [antidote, sync_log, true]),

            {ok, _} = rpc:call(Node, application, ensure_all_started, [saturn_leaf]),
            ct:pal("Node ~p started with ports ~p-~p", [Node, Port, Port + 4]),

            {ready, Node};
        {error, already_started, Node} ->
            ct:log("Node ~p already started, reusing node", [Node]),
            {ready, Node};
        {error, Reason, Node} ->
            ct:pal("Error starting node ~w, reason ~w, will retry", [Node, Reason]),
            ct_slave:stop(Name),
            time_utils:wait_until_offline(Node),
            start_node(Name, Config)
    end.


%% @doc Forces shutdown of nodes and restarts them again with given configuration
-spec kill_and_restart_nodes([node()], [tuple()]) -> [node()].
kill_and_restart_nodes(NodeList, Config) ->
    NewNodeList = brutal_kill_nodes(NodeList),
    restart_nodes(NewNodeList, Config).


%% @doc Kills all given nodes, crashes if one node cannot be stopped
-spec kill_nodes([node()]) -> [node()].
kill_nodes(NodeList) ->
    lists:map(fun(Node) -> {ok, Name} = ct_slave:stop(get_node_name(Node)), Name end, NodeList).


%% @doc Send force kill signals to all given nodes
-spec brutal_kill_nodes([node()]) -> [node()].
brutal_kill_nodes(NodeList) ->
    lists:map(fun(Node) ->
                  ct:pal("Killing node ~p", [Node]),
                  OSPidToKill = rpc:call(Node, os, getpid, []),
                  %% try a normal kill first, but set a timer to
                  %% kill -9 after X seconds just in case
%%                  rpc:cast(Node, timer, apply_after,
%%                      [?FORCE_KILL_TIMER, os, cmd, [io_lib:format("kill -9 ~s", [OSPidToKill])]]),
                  ct_slave:stop(get_node_name(Node)),
                  rpc:cast(Node, os, cmd, [io_lib:format("kill -15 ~s", [OSPidToKill])]),
                  Node
              end, NodeList).


%% @doc Restart nodes with given configuration
-spec restart_nodes([node()], [tuple()]) -> [node()].
restart_nodes(NodeList, Config) ->
    pmap(fun(Node) ->
        ct:pal("Restarting node ~p", [Node]),

        ct:log("Starting and waiting until vnodes are restarted at node ~w", [Node]),
        start_node(get_node_name(Node), Config),

        ct:log("Waiting until ring converged @ ~p", [Node]),
        riak_utils:wait_until_ring_converged([Node]),

        ct:log("Waiting until ready @ ~p", [Node]),
        time_utils:wait_until(Node, fun wait_init:check_ready/1),
        Node
         end, NodeList).


%% @doc Convert node to node atom
-spec get_node_name(node()) -> atom().
get_node_name(NodeAtom) ->
    Node = atom_to_list(NodeAtom),
    {match, [{Pos, _Len}]} = re:run(Node, "@"),
    list_to_atom(string:substr(Node, 1, Pos)).


%% @doc TODO
-spec pmap(fun(), list()) -> list().
pmap(F, L) ->
    Parent = self(),
    lists:foldl(
        fun(X, N) ->
            spawn_link(fun() ->
                           Parent ! {pmap, N, F(X)}
                       end),
            N+1
        end, 0, L),
    L2 = [receive {pmap, N, R} -> {N, R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.


partition_cluster(ANodes, BNodes) ->
    pmap(fun({Node1, Node2}) ->
                true = rpc:call(Node1, erlang, set_cookie, [Node2, canttouchthis]),
                true = rpc:call(Node1, erlang, disconnect_node, [Node2]),
                ok = time_utils:wait_until_disconnected(Node1, Node2)
        end,
         [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.


heal_cluster(ANodes, BNodes) ->
    GoodCookie = erlang:get_cookie(),
    pmap(fun({Node1, Node2}) ->
                true = rpc:call(Node1, erlang, set_cookie, [Node2, GoodCookie]),
                ok = time_utils:wait_until_connected(Node1, Node2)
        end,
         [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.



web_ports(dev1) -> 10015;
web_ports(dev2) -> 10025;
web_ports(dev3) -> 10035;
web_ports(dev4) -> 10045;
web_ports(clusterdev1) -> 10115;
web_ports(clusterdev2) -> 10125;
web_ports(clusterdev3) -> 10135;
web_ports(clusterdev4) -> 10145;
web_ports(clusterdev5) -> 10155;
web_ports(clusterdev6) -> 10165;
web_ports(dcdev1) -> 10215;
web_ports(dcdev2) -> 10225;
web_ports(dcdev3) -> 10235.


%% Build clusters for all test suites.
set_up_clusters_common(Config) ->
    ClusterAndDcConfiguration = [[dev1, dev2], [dev3], [dev4]],

    StartDCs = fun(Nodes) ->
        %% start each node
        Cl = pmap(fun(N) ->
            start_node(N, Config)
                  end,
            Nodes),
        [{Status, Claimant} | OtherNodes] = Cl,

        %% check if node was reused or not
        case Status of
            ready -> ok;
            connect ->
                ct:pal("Creating a ring for claimant ~p and other nodes ~p", [Claimant, unpack(OtherNodes)]),
                ok = rpc:call(Claimant, antidote_dc_manager, add_nodes_to_dc, [unpack(Cl)])
        end,
        Cl
               end,

    Clusters = pmap(fun(Cluster) ->
        StartDCs(Cluster)
                    end, ClusterAndDcConfiguration),

    %% DCs started, but not connected yet
    pmap(fun([{Status, MainNode} | _] = CurrentCluster) ->
        case Status of
            ready -> ok;
            connect ->
                ct:pal("~p of ~p subscribing to other external DCs", [MainNode, unpack(CurrentCluster)]),

                Descriptors = lists:map(fun([{_Status, FirstNode} | _]) ->
                    {ok, Descriptor} = rpc:call(FirstNode, antidote_dc_manager, get_connection_descriptor, []),
                    Descriptor
                                        end, Clusters),

                %% subscribe to descriptors of other dcs
                ok = rpc:call(MainNode, antidote_dc_manager, subscribe_updates_from, [Descriptors])
        end
         end, Clusters),


    ct:log("Clusters joined and data centers connected connected: ~p", [ClusterAndDcConfiguration]),
    [unpack(DC) || DC <- Clusters].


bucket(BucketBaseAtom) ->
    BucketRandomSuffix = [rand:uniform(127)],
    Bucket = list_to_atom(atom_to_list(BucketBaseAtom) ++ BucketRandomSuffix),
    ct:log("Using random bucket: ~p", [Bucket]),
    Bucket.


%% logger configuration for each level
%% see http://erlang.org/doc/man/logger.html
log_config(LogDir) ->
    DebugConfig = #{level => debug,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "debug.log")}}},

    InfoConfig = #{level => info,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "info.log")}}},

    NoticeConfig = #{level => notice,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "notice.log")}}},

    WarningConfig = #{level => warning,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "warning.log")}}},

    ErrorConfig = #{level => error,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "error.log")}}},

    [
        {handler, debug_antidote, logger_std_h, DebugConfig},
        {handler, info_antidote, logger_std_h, InfoConfig},
        {handler, notice_antidote, logger_std_h, NoticeConfig},
        {handler, warning_antidote, logger_std_h, WarningConfig},
        {handler, error_antidote, logger_std_h, ErrorConfig}
    ].

-spec unpack([{ready | connect, atom()}]) -> [atom()].
unpack(NodesWithStatus) ->
    [Node || {_Status, Node} <- NodesWithStatus].

server_name(Node)->
    {saturn_client_receiver, Node}.
    %{global, list_to_atom(atom_to_list(Node) ++ atom_to_list(saturn_client_receiver))}.

wait_for_name(Name)->
    wait_for_name(Name, false).

wait_for_name(_,true) ->
    ok;

wait_for_name(Name, false) ->
    wait_for_name(Name, lists:member(Name, global:registered_names())).

is_ring_ready(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            riak_core_ring:ring_ready(Ring);
        _ ->
            false
    end.
wait_until_ring_ready(Node) ->
    wait_until_ring_ready(Node, 10, 500).

wait_until_ring_ready(Node, Retry, Delay) ->
    Res = is_ring_ready(Node) ,
    case Res of
        true ->
            ok;
        _ when Retry == 1 ->
            {fail, Res};
        _ ->
            timer:sleep(Delay),
            wait_until_ring_ready(Node, Retry-1, Delay)
    end.
