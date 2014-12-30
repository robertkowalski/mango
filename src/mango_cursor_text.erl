-module(mango_cursor_text).

-export([
    create/3,
    execute/3
]).


-include_lib("couch/include/couch_db.hrl").
-include_lib("dreyfus/include/dreyfus.hrl").
-include("mango_cursor.hrl").
-include("mango.hrl").


create(Db, Selector0, Opts) ->
    Selector = mango_selector:normalize(Selector0),
    Fields = case couch_util:get_value(fields, Opts, all_fields) of
        all_fields -> [];
        <<"all_fields">> -> [];
        Else -> Else
    end,
    IndexFields = [<<"$default">> | Fields],
    ExistingIndexes = mango_idx:filter_list(mango_idx:list(Db), [<<"text">>]),
    UsableIndexes = find_usable_indexes(IndexFields, ExistingIndexes),
    if UsableIndexes /= [] -> ok; true ->
        ?MANGO_ERROR({no_usable_index, operator_unsupported})
    end,
    SortIndexes = mango_cursor:get_sort_indexes(ExistingIndexes, UsableIndexes,
        Opts),
    Index = choose_best_index(SortIndexes, IndexFields),
    Limit = couch_util:get_value(limit, Opts, 50),
    %% Currently set the Limit at 50. We want to set this
    %% in mango_opts to be consistent with view queries.
    CapLimit = case Limit > 50 of
        true -> 50;
        false -> Limit
    end,
    Skip = couch_util:get_value(skip, Opts, 0),

    {ok, #cursor{
        db = Db,
        index = Index,
        ranges = null,
        selector = Selector,
        opts = Opts,
        limit = CapLimit,
        skip = Skip,
        fields = IndexFields
    }}.


execute(#cursor{db = Db, index = Idx, limit=Limit, opts=Opts} = Cursor0,
        UserFun, UserAcc) ->
    DbName = Db#db.name,
    DDoc = ddocid(Idx),
    IndexName = mango_idx:name(Idx),
    Query = mango_selector_text:convert(Cursor0#cursor.selector),
    SortQuery = sort_query(Opts),
    Bookmark0 = case get_bookmark(Opts) of
        <<>> -> nil;
        Else -> Else
    end,
    QueryArgs = #index_query_args{
        q = Query,
        include_docs = true,
        limit = Limit,
        sort = SortQuery,
        bookmark = Bookmark0
    },
    case dreyfus_fabric_search:go(DbName, DDoc, IndexName, QueryArgs) of
        {ok, Bookmark1, _, Hits0, _, _} ->
            Hits = hits_to_json(DbName, Hits0, Cursor0#cursor.selector),
            Bookmark = dreyfus_fabric_search:pack_bookmark(Bookmark1),
            {ok, UserAcc1} = UserFun({add_key, bookmark, Bookmark}, UserAcc),
            try
                UserAcc2 = lists:foldl(fun(Hit, Acc) ->
                    case UserFun({row, Hit}, Acc) of
                        {ok, NewAcc} -> NewAcc;
                        {stop, NewAcc} -> throw({stop, NewAcc})
                    end
                end, UserAcc1, Hits),
                {ok, UserAcc2}
            catch
                throw:{stop, StopAcc} ->
                    {ok, StopAcc}
            end;
        {error, Reason} ->
            ?MANGO_ERROR({text_search_error, {error, Reason}})
    end.


%% Convert Query to Dreyfus sort specifications
%% Covert <<"Field">>, <<"desc">> to <<"-Field">>
%% and append to the dreyfus query
sort_query(Opts) ->
    {sort, {Sort}} = lists:keyfind(sort, 1, Opts),
    SortList = lists:map(fun(SortField) ->
        case SortField of
            {Field, <<"asc">>} -> Field;
            {Field, <<"desc">>} -> <<"-", Field/binary>>;
            Field when is_binary(Field) -> Field
        end
    end, Sort),
    case SortList of
        [] -> relevance;
        _ -> SortList
    end.


get_bookmark(Opts) ->
    {bookmark, Bookmark} = lists:keyfind(bookmark, 1, Opts),
    Bookmark.


ddocid(Idx) ->
    case mango_idx:ddoc(Idx) of
        <<"_design/", Rest/binary>> ->
            Rest;
        Else ->
            Else
    end.


find_usable_indexes(Possible, []) ->
    ?MANGO_ERROR({no_usable_index, {fields, Possible}});
%% If the user did not specify any fields, then return all existing text indexes
find_usable_indexes([<<"$default">>], Existing) ->
    Existing;
find_usable_indexes(Possible, Existing) ->
    Usable = lists:foldl(fun(Idx, Acc) ->
        Columns = mango_idx:columns(Idx),
        %% Check to see if any of the Columns exist in our Possible Fields
        case sets:is_subset(sets:from_list(Possible),
            sets:from_list(Columns)) of
            true ->
                [Idx | Acc];
            false ->
                Acc
        end
    end, [], Existing),
    if length(Usable) > 0 -> ok; true ->
        ?MANGO_ERROR({no_usable_index, {fields, Possible}})
    end,
    Usable.


%% If no field list exists, we choose the an index that has <<"all_fields">>
%% or any of the text indexes
choose_best_index(Indexes, IndexFields) ->
    case IndexFields of
        [<<"$default">>] ->
            lists:foldl(fun(Idx, Acc) ->
                case mango_idx:columns(Acc) of
                    all_fields -> Idx;
                    _ -> Acc
                end
            end, hd(Indexes), Indexes);
        _ ->
            hd(Indexes)
    end.


hits_to_json(DbName, Hits, Selector) ->
    {Ids, HitData} = lists:unzip(lists:map(fun get_hit_data/1, Hits)),
    {ok, JsonDocs0} = dreyfus_fabric:get_json_docs(DbName, Ids),
    JsonDocs = filter_text_results(JsonDocs0, Selector),
    lists:zipwith(fun({Id, Order, Fields}, {Id, Doc}) ->
        {[{id, Id}, {order, Order}, {fields, {Fields}}, Doc]}
    end, HitData, JsonDocs).


get_hit_data(Hit) ->
    Id = couch_util:get_value(<<"_id">>, Hit#hit.fields),
    Fields = lists:keydelete(<<"_id">>, 1, Hit#hit.fields),
    {Id, {Id, Hit#hit.order, Fields}}.


filter_text_results(Docs, Selector) ->
    Pred = fun({_, {doc, Doc}}) -> mango_selector:match(Selector, Doc) end,
    lists:filter(Pred, Docs).
