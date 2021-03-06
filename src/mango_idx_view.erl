% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
% http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(mango_idx_view).


-export([
    validate/1,
    add/2,
    remove/2,
    from_ddoc/1,
    to_json/1,
    columns/1,
    start_key/1,
    end_key/1
]).


-include_lib("couch/include/couch_db.hrl").
-include("mango.hrl").
-include("mango_idx.hrl").


validate(#idx{}=Idx) ->
    {ok, Def} = do_validate(Idx#idx.def),
    {ok, Idx#idx{def=Def}}.


add(#doc{body={Props0}}=DDoc, Idx) ->
    Views1 = case proplists:get_value(<<"views">>, Props0) of
        {Views0} -> Views0;
        _ -> []
    end,
    NewView = make_view(Idx),
    Views2 = lists:keystore(element(1, NewView), 1, Views1, NewView),
    Props1 = lists:keystore(<<"views">>, 1, Props0, {<<"views">>, {Views2}}),
    {ok, DDoc#doc{body={Props1}}}.


remove(#doc{body={Props0}}=DDoc, Idx) ->
    Views1 = case proplists:get_value(<<"views">>, Props0) of
        {Views0} ->
            Views0;
        _ ->
            ?MANGO_ERROR({index_not_found, Idx#idx.name})
    end,
    Views2 = lists:keydelete(Idx#idx.name, 1, Views1),
    if Views2 /= Views1 -> ok; true ->
        ?MANGO_ERROR({index_not_found, Idx#idx.name})
    end,
    Props1 = case Views2 of
        [] ->
            lists:keydelete(<<"views">>, 1, Props0);
        _ ->
            lists:keystore(<<"views">>, 1, Props0, {<<"views">>, {Views2}})
    end,
    {ok, DDoc#doc{body={Props1}}}.


from_ddoc({Props}) ->
    case lists:keyfind(<<"views">>, 1, Props) of
        {<<"views">>, {Views}} when is_list(Views) ->
            lists:flatmap(fun({Name, {VProps}}) ->
                Def = proplists:get_value(<<"map">>, VProps),
                {Opts0} = proplists:get_value(<<"options">>, VProps),
                Opts = lists:keydelete(<<"sort">>, 1, Opts0),
                I = #idx{
                    type = <<"json">>,
                    name = Name,
                    def = Def,
                    opts = Opts
                },
                % TODO: Validate the index definition
                [I]
            end, Views);
        _ ->
            []
    end.


to_json(Idx) ->
    {[
        {ddoc, Idx#idx.ddoc},
        {name, Idx#idx.name},
        {type, Idx#idx.type},
        {def, {def_to_json(Idx#idx.def)}}
    ]}.


columns(Idx) ->
    {Props} = Idx#idx.def,
    {<<"fields">>, {Fields}} = lists:keyfind(<<"fields">>, 1, Props),
    [Key || {Key, _} <- Fields].


start_key([]) ->
    [];
start_key([{'$gt', Key, _, _} | Rest]) ->
    case mango_json:special(Key) of
        true ->
            [];
        false ->
            [Key | start_key(Rest)]
    end;
start_key([{'$gte', Key, _, _} | Rest]) ->
    false = mango_json:special(Key),
    [Key | start_key(Rest)];
start_key([{'$eq', Key, '$eq', Key} | Rest]) ->
    false = mango_json:special(Key),
    [Key | start_key(Rest)].


end_key([]) ->
    [{[]}];
end_key([{_, _, '$lt', Key} | Rest]) ->
    case mango_json:special(Key) of
        true ->
            [{[]}];
        false ->
            [Key | end_key(Rest)]
    end;
end_key([{_, _, '$lte', Key} | Rest]) ->
    false = mango_json:special(Key),
    [Key | end_key(Rest)];
end_key([{'$eq', Key, '$eq', Key} | Rest]) ->
    false = mango_json:special(Key),
    [Key | end_key(Rest)].


do_validate({Props}) ->
    {ok, Opts} = mango_opts:validate(Props, opts()),
    {ok, {Opts}};
do_validate(Else) ->
    ?MANGO_ERROR({invalid_index_json, Else}).


def_to_json({Props}) ->
    def_to_json(Props);
def_to_json([]) ->
    [];
def_to_json([{fields, Fields} | Rest]) ->
    [{<<"fields">>, mango_sort:to_json(Fields)} | def_to_json(Rest)];
def_to_json([{<<"fields">>, Fields} | Rest]) ->
    [{<<"fields">>, mango_sort:to_json(Fields)} | def_to_json(Rest)];
def_to_json([{Key, Value} | Rest]) ->
    [{Key, Value} | def_to_json(Rest)].


opts() ->
    [
        {<<"fields">>, [
            {tag, fields},
            {validator, fun mango_opts:validate_sort/1}
        ]}
    ].


make_view(Idx) ->
    View = {[
        {<<"map">>, Idx#idx.def},
        {<<"reduce">>, <<"_count">>},
        {<<"options">>, {Idx#idx.opts}}
    ]},
    {Idx#idx.name, View}.
