-module(pivot_mab_arms_db_riak).

-export([add/4]).
-export([remove/4]).
-export([enable/4]).
-export([disable/4]).
-export([all/3]).
-export([enabled/3]).

-define(BUCKET(Env), <<"pivot-mab-arms-", Env/binary>>).
-define(KEY(App, Bandit, Arm), <<App/binary, ":", Bandit/binary, ":", Arm/binary>>).
-define(SECONDARY_INDEX(App, Bandit), binary_to_list(<<App/binary, Bandit/binary>>)).

-include_lib("riakc/include/riakc.hrl").

add(Env, App, Bandit, Arm) ->
  disable(Env, App, Bandit, Arm).

remove(Env, App, Bandit, Arm) ->
  riakou:do(delete, [?BUCKET(Env), ?KEY(App, Bandit, Arm)]).

enable(Env, App, Bandit, Arm) ->
  riakou:do(put, [obj(Env, App, Bandit, Arm, 1)]).

disable(Env, App, Bandit, Arm) ->
  riakou:do(put, [obj(Env, App, Bandit, Arm, 0)]).

all(Env, App, Bandit) ->
  extract_ids(App, Bandit, riakou:do(get_index, [?BUCKET(Env), {integer_index, ?SECONDARY_INDEX(App, Bandit)}, 0, 1])).

enabled(Env, App, Bandit) ->
  extract_ids(App, Bandit, riakou:do(get_index, [?BUCKET(Env), {integer_index, ?SECONDARY_INDEX(App, Bandit)}, 1])).

obj(Env, App, Bandit, Arm, Enabled) ->
  Obj = riakc_obj:new(?BUCKET(Env), ?KEY(App, Bandit, Arm), <<1>>),
  MD1 = riakc_obj:get_update_metadata(Obj),
  MD2 = riakc_obj:set_secondary_index(MD1, [
    {{integer_index, ?SECONDARY_INDEX(App, Bandit)}, [Enabled]}
  ]),
  riakc_obj:update_metadata(Obj, MD2).

extract_ids(App, Bandit, {ok, {keys, Keys}}) ->
  {ok, [begin
    PrefixLength = byte_size(App) + byte_size(Bandit) + 2,
    ArmLength = byte_size(Key) - PrefixLength,
    binary:part(Key, {PrefixLength, ArmLength})
  end || Key <- Keys]};
extract_ids(App, Bandit, {ok, Rec}) ->
  extract_ids(App, Bandit, {ok, {keys, Rec?INDEX_RESULTS.keys}});
extract_ids(_, _, Error) ->
  Error.
