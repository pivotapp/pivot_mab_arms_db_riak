-module(pivot_mab_arms_db_riak_test).

-include_lib ("eunit/include/eunit.hrl").

basic_test() ->
  ok = riakou:start(),
  riakou:start_link(<<"riak://localhost">>),

  % We have to wait for us to connect to riak... :/
  timer:sleep(200),

  Env = <<"test">>,
  App = <<"app">>,
  Bandit = <<"button">>,
  Arms = [<<"red">>, <<"blue">>, <<"green">>],

  [ok, ok, ok] = [pivot_mab_arms_db_riak:add(Env, App, Bandit, Arm) || Arm <- Arms],
  {ok, AllArms} = pivot_mab_arms_db_riak:all(Env, App, Bandit),
  ?assert(check_set_equality(AllArms, Arms)),

  {ok, []} = pivot_mab_arms_db_riak:enabled(Env, App, Bandit),

  ok = pivot_mab_arms_db_riak:enable(Env, App, Bandit, <<"red">>),

  {ok, [<<"red">>]} = pivot_mab_arms_db_riak:enabled(Env, App, Bandit),

  {ok, AllArms2} = pivot_mab_arms_db_riak:all(Env, App, Bandit),
  ?assert(check_set_equality(AllArms2, Arms)),

  ok = pivot_mab_arms_db_riak:disable(Env, App, Bandit, <<"red">>),
  {ok, []} = pivot_mab_arms_db_riak:enabled(Env, App, Bandit),
  
  [ok, ok, ok] = [pivot_mab_arms_db_riak:remove(Env, App, Bandit, Arm) || Arm <- Arms],

  {ok, []} = pivot_mab_arms_db_riak:all(Env, App, Bandit),

  ok.

check_set_equality(List1, List2) ->
  gb_sets:is_empty(gb_sets:difference(gb_sets:from_list(List1), gb_sets:from_list(List2))).
