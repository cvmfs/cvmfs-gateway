%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_be_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([api_qc/1]).

-export([valid_user_valid_path/1
        ,valid_user_busy_path/1
        ,invalid_user_valid_path/1
        ,valid_user_invalid_path/1
        ,invalid_user_invalid_path/1]).

-export([end_valid_lease/1
        ,end_invalid_lease/1
        ,end_lease_invalid_macaroon/1]).

-export([lease_success/1
        ,submission_with_invalid_token_fails/1
        ,submission_with_expired_token_fails/1]).


%% Tests description

all() ->
    [{group, specifications}
    ,{group, properties}].

groups() ->
    [{specifications, [], [{group, new_lease}
                          ,{group, end_lease}
                          ,{group, submit_payload}]}
    ,{new_lease, [], [valid_user_valid_path
                     ,valid_user_busy_path
                     ,invalid_user_valid_path
                     ,valid_user_invalid_path
                     ,invalid_user_invalid_path]}
    ,{end_lease, [], [end_valid_lease
                     ,end_invalid_lease
                     ,end_lease_invalid_macaroon]}
    ,{submit_payload, [], [lease_success
                          ,submission_with_invalid_token_fails
                          ,submission_with_expired_token_fails]}
    ,{properties, [], [api_qc]}].

%% Set up and tear down
init_per_suite(Config) ->
    application:load(mnesia),
    application:set_env(mnesia, schema_location, ram),
    application:ensure_all_started(mnesia),

    ok = application:load(cvmfs_services),
    ok = ct:require(repos),
    ok = ct:require(acl),
    ok = application:set_env(cvmfs_services, enabled_services, [cvmfs_auth, cvmfs_lease, cvmfs_be]),
    ok = application:set_env(cvmfs_services, repo_config, #{repos => ct:get_config(repos)
                                                           ,acl => ct:get_config(acl)}),

    MaxLeaseTime = 50, % milliseconds
    ok = application:set_env(cvmfs_services, max_lease_time, MaxLeaseTime),

    {ok, _} = application:ensure_all_started(cvmfs_services),

    lists:flatten([[{max_lease_time, MaxLeaseTime}]
                  ,Config]).

end_per_suite(_Config) ->
    application:stop(cvmfs_services),
    application:unload(cvmfs_services),
    application:stop(mnesia),
    application:unload(mnesia),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% Specs

% New lease
% Valid user and valid path should be accepted
valid_user_valid_path(_Config) ->
    {VUser, VPath} = valid_user_and_path(),
    {ok, Token} = cvmfs_be:new_lease(VUser, VPath),
    ok = cvmfs_be:end_lease(Token).
% Valid user and busy path should be rejected with remaining time
valid_user_busy_path(_Config) ->
    {VUser, VPath} = valid_user_and_path(),
    {ok, Token} = cvmfs_be:new_lease(VUser, VPath),
    {path_busy, _} = cvmfs_be:new_lease(VUser, VPath),
    ok = cvmfs_be:end_lease(Token).
% Invalid user and valid path should be rejected
invalid_user_valid_path(_Config) ->
    {VUser, VPath} = valid_user_and_path(),
    {IUser, _} = invalid_user_and_path(VUser, VPath),
    {error, invalid_user} = cvmfs_be:new_lease(IUser, VPath).
% Valid user and invalid path should be rejected
valid_user_invalid_path(_Config) ->
    {VUser, VPath} = valid_user_and_path(),
    {_, IPath} = invalid_user_and_path(VUser, VPath),
    {error, invalid_path} = cvmfs_be:new_lease(VUser, IPath).
% Invalid user and invalid path should be rejected with {error, invalid_user}
invalid_user_invalid_path(_Config) ->
    {VUser, VPath} = valid_user_and_path(),
    {IUser, IPath} = invalid_user_and_path(VUser, VPath),
    {error, invalid_user} = cvmfs_be:new_lease(IUser, IPath).

% End lease
% End valid lease
end_valid_lease(_Config) ->
    {VUser, VPath} = valid_user_and_path(),
    {ok, Token} = cvmfs_be:new_lease(VUser, VPath),
    ok = cvmfs_be:end_lease(Token).

% End invalid lease
end_invalid_lease(_Config) ->
    {VUser, VPath} = valid_user_and_path(),
    {ok, Token} = cvmfs_be:new_lease(VUser, VPath),
    ok = cvmfs_be:end_lease(Token),
    ok = cvmfs_be:end_lease(Token).

% End lease invalid macaroon
end_lease_invalid_macaroon(_Config) ->
    Token = <<"fake_token">>,
    {error, invalid_macaroon} = cvmfs_be:end_lease(Token).

% Submit payload
% Normal lease check
lease_success(_Config) ->
    % Start with a valid user and path and receive a valid lease token
    {User, Path} = valid_user_and_path(),
    Payload = <<"placeholder_for_a_real_payload">>,
    {ok, Token} = cvmfs_be:new_lease(User, Path),
    % Followup with a payload submission
    {ok, payload_added} = cvmfs_be:submit_payload(Token, Payload),
    % Submit final payload and end the lease
    {ok, payload_added} = cvmfs_be:submit_payload(Token, Payload),
    ok = cvmfs_be:end_lease(Token),
    % After the lease has been closed, the token should be rejected
    {error, invalid_lease} = cvmfs_be:submit_payload(Token, Payload).

% Attempt to submit a payload without first obtaining a token
submission_with_invalid_token_fails(_Config) ->
    Token = <<"invalid_token">>,
    Payload = <<"placeholder">>,
    {error, invalid_macaroon} = cvmfs_be:submit_payload(Token, Payload).

% Start a valid lease, make submission after the token has expired
submission_with_expired_token_fails(Config) ->
    {User, Path} = valid_user_and_path(),
    Payload = <<"placeholder">>,
    {ok, Token} = cvmfs_be:new_lease(User, Path),
    ct:sleep(?config(max_lease_time, Config)),
    {error, lease_expired} = cvmfs_be:submit_payload(Token, Payload).


%% Properties
api_qc(_Config) ->
    ?assert(true).

%% Private functions
valid_user_and_path() ->
    [U | _ ] = lists:filter(fun(U1) ->
                                    {ok, Ps} = cvmfs_auth:get_user_permissions(U1),
                                    length(Ps) > 0
                            end,
                            cvmfs_auth:get_users()),
    {ok, [P | _]} = cvmfs_auth:get_user_permissions(U),
    {U, P}.

%% Produces an invalid {User, Path} pair
invalid_user_and_path(VUser, VPath) ->
    {<<VUser/binary,"_invalid">>, <<VPath/binary,"_invalid">>}.