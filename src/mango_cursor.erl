-module(mango_cursor).


-export([
    create/3,
    execute/3
]).


-include_lib("couch/include/couch_db.hrl").
-include("mango.hrl").
-include("mango_cursor.hrl").


-define(SUPERVISOR, mango_cursor_sup).


create(Db, Selector0, Opts) ->
    Selector = mango_selector:normalize(Selector0),

    ExistingIndexes = mango_idx:list(Db),
    if ExistingIndexes /= [] -> ok; true ->
        ?MANGO_ERROR({no_usable_index, no_indexes_defined})
    end,
    
    FilteredIndexes = maybe_filter_indexes(ExistingIndexes, Opts),
    if FilteredIndexes /= [] -> ok; true ->
        ?MANGO_ERROR({no_usable_index, no_index_matching_name})
    end,
    
    SortIndexes = mango_idx:for_sort(FilteredIndexes, Opts),
    if SortIndexes /= [] -> ok; true ->
        ?MANGO_ERROR({no_usable_index, missing_sort_index})
    end,
    
    UsableFilter = fun(I) -> mango_idx:is_usable(I, Selector) end,
    UsableIndexes = lists:filter(UsableFilter, SortIndexes),
    if UsableIndexes /= [] -> ok; true ->
        ?MANGO_ERROR({no_usable_index, selector_unsupported})
    end,

    create_cursor(Db, UsableIndexes, Selector, Opts).


execute(#cursor{index=Idx}=Cursor, UserFun, UserAcc) ->
    Mod = mango_idx:cursor_mod(Idx),
    Mod:execute(Cursor, UserFun, UserAcc).


maybe_filter_indexes(Indexes, Opts) ->
    case lists:keyfind(use_index, 1, Opts) of
        {use_index, []} ->
            Indexes;
        {use_index, [DesignId]} ->
            filter_indexes(Indexes, DesignId);
        {use_index, [DesignId, ViewName]} ->
            filter_indexes(Indexes, DesignId, ViewName)
    end.


filter_indexes(Indexes, DesignId0) ->
    DesignId = case DesignId0 of
        <<"_design/", _/binary>> ->
            DesignId0;
        Else ->
            <<"_design/", Else/binary>>
    end,
    FiltFun = fun(I) -> mango_idx:ddoc(I) == DesignId end,
    lists:filter(FiltFun, Indexes).


filter_indexes(Indexes0, DesignId, ViewName) ->
    Indexes = filter_indexes(Indexes0, DesignId),
    FiltFun = fun(I) -> mango_idx:name(I) == ViewName end,
    lists:filter(FiltFun, Indexes).


create_cursor(Db, Indexes, Selector, Opts) ->
    [{CursorMod, Indexes} | _] = group_indexes_by_type(Indexes),
    CursorMod:create(Db, Indexes, Selector, Opts).


group_indexes_by_type(Indexes) ->
    IdxDict = lists:foldl(fun(I, D) ->
        dict:append(mango_idx:cursor_mod(I), I, D)
    end, dict:new(), Indexes),
    % The first cursor module that has indexes will be
    % used to service this query.
    CursorModules = [
        mango_cursor_view,
        mango_cursor_text
    ],
    lists:flatmap(fun(CMod) ->
        case dict:find(CMod, IdxDict) of
            {ok, Indexes} ->
                [{CMod, Indexes}];
            error ->
                []
        end
    end, CursorModules).
