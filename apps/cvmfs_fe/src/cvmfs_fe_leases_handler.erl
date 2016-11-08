%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_fe_lease_handler
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_fe_leases_handler).

-export([init/2]).

init(Req0 = #{method := <<"GET">>}, State) ->
    Req = cowboy_req:reply(200,
                           #{<<"content-type">> => <<"text/plain">>},
                           <<"Leases!">>,
                           Req0),
    {ok, Req, State};
init(Req0 = #{method := <<"PUT">>}, State) ->

    {ok, Req0, State}.
