-module(mango_cursor_text).

-export([
    create/4,
    explain/1,
    execute/3
]).


-include_lib("couch/include/couch_db.hrl").
-include_lib("dreyfus/include/dreyfus.hrl").
-include("mango_cursor.hrl").
-include("mango.hrl").


create(Db, Indexes, Selector, Opts) ->
    Index = case Indexes of
        [Index0] ->
            Index0;
        _ ->
            ?MANGO_ERROR(multiple_text_indexes)
    end,

    % Limit the result set size to 50 for Clouseau's
    % sake. We may want to revisit this.
    Limit0 = couch_util:get_value(limit, Opts, 50),
    Limit = if Limit0 < 50 -> Limit0; true -> 50 end,
    Skip = couch_util:get_value(skip, Opts, 0),
    Fields = couch_util:get_value(fields, Opts, all_fields),

    {ok, #cursor{
        db = Db,
        index = Index,
        ranges = null,
        selector = Selector,
        opts = Opts,
        limit = Limit,
        skip = Skip,
        fields = Fields
    }}.


explain(Cursor) ->
    #cursor{
        selector = Selector,
        opts = Opts
    } = Cursor,
    [
        {'query', mango_selector_text:convert(Selector)},
        {sort, sort_query(Opts)}
    ].


execute(Cursor, UserFun, UserAcc) ->
    #cursor{
        db = Db,
        index = Idx,
        limit = Limit,
        selector = Selector,
        opts = Opts
    } = Cursor,
    DbName = Db#db.name,
    DDoc = ddocid(Idx),
    IndexName = mango_idx:name(Idx),
    Query = mango_selector_text:convert(Selector),
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
        bookmark = Bookmark0,
        keep_sortable=true
    },
    {Hits, Bookmark} = get_hits(DbName, DDoc, IndexName, QueryArgs, Selector,
        Limit, []),
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
    end.

%% Returns hits required by the limit. We keep calling search
%% until we meet the required limit value.
get_hits(DbName, DDoc, IndexName, QueryArgs, Selector, Difference, HitsAcc) ->
    case dreyfus_fabric_search:go(DbName, DDoc, IndexName, QueryArgs) of
        {ok, Bookmark1, _, Hits0, _, _} ->
            NewHits = hits_to_json(DbName, Hits0, Selector),
            DreyfusDiff = Difference - length(Hits0),
            FilteredDiff = Difference - length(NewHits),
            case DreyfusDiff of
                % We don't care if we get filtered because there are no results
                % left anyway, so just return current and new hits and bookmark
                DD when DD > 0 ->
                    Bookmark = dreyfus_fabric_search:pack_bookmark(Bookmark1),
                    {HitsAcc ++ NewHits, Bookmark};
                % We get dreyfus results more than or equal to our difference
                _ ->
                    case FilteredDiff of
                        % Our filtered results has less than required difference
                        % so get another block
                        FD when FD > 0 ->
                            get_hits(DbName, DDoc, IndexName, QueryArgs,
                                Selector, FD, HitsAcc ++ NewHits);
                        % Our filtered results is more than required so need
                        % to add the remaining hits
                        _ ->
                            add_missing_hits(Difference, HitsAcc, NewHits)
                    end
            end;
        {error, Reason} ->
            ?MANGO_ERROR({text_search_error, {error, Reason}})
    end.


%% Populates the remaining left over rows that were filtered out
%% Also grabs the bookmark for the last row
add_missing_hits(Diff, HitsAcc, NewHits) ->
    DiffHits = remove_sortable(lists:sublist(NewHits, Diff)),
    Bookmark = dreyfus_fabric_search:pack_bookmark(lists:nth(Diff, NewHits)),
    {HitsAcc ++ DiffHits, Bookmark}.


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


remove_sortable(List) ->
    remove_sortable(List, []).

remove_sortable([], Acc) ->
    lists:reverse(Acc);
remove_sortable([#sortable{item=Item} | Rest], Acc) ->
    remove_sortable(Rest, [Item | Acc]).


hits_to_json(DbName, Hits, Selector) ->
    Ids = lists:map(fun(Hit) ->
        couch_util:get_value(<<"_id">>, Hit#hit.fields)
    end, remove_sortable(Hits)),
    {ok, IdDocs} = dreyfus_fabric:get_json_docs(DbName, Ids),
    Docs = lists:map(fun({_Id, {doc, Doc}}) ->
        Doc
    end, IdDocs),
    filter_text_results(Docs, Selector).


filter_text_results(Docs, Selector) ->
    Pred = fun(Doc) -> mango_selector:match(Selector, Doc) end,
    lists:filter(Pred, Docs).
