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

    ExistingIndexes = mango_idx:list(Db, Selector, Opts),
    SortIndexes = mango_idx:for_sort(ExistingIndexes, Opts),
    UsableFilter = fun(I) -> mango_idx:is_usable(I, Selector) end,
    UsableIndexes = lists:filter(UsableFilter, SortIndexes),

    if UsableIndexes /= [] -> ok; true ->
        ?MANGO_ERROR({no_usable_index, selector_unsupported})
    end,

    Index = choose_best(UsableIndexes, Selector, Opts),    

    Mod = mango_idx:cursor_mod(Index),
    Mod:create(Db, Index, Selector, Opts).


execute(#cursor{index=Idx}=Cursor, UserFun, UserAcc) ->
    Mod = mango_idx:cursor_mod(Idx),
    Mod:execute(Cursor, UserFun, UserAcc).


choose_best(Indexes, Selector, Opts) ->
    Pred = fun(I) -> {mango_idx:priority(I, Selector, Opts), I} end,
    Prioritized = lists:map(Pred, Indexes),
    Sorted = lists:sort(Prioritized),
    {_, Idx} = lists:last(Sorted),
    Idx.
