-module(mango_text_selector).


-export([
    parse_selector/1
]).


-include_lib("couch/include/couch_db.hrl").
-include("mango.hrl").


parse_selector(Value) when is_binary(Value) ->
    BinVal = get_value(Value),
    <<"\\:string:", BinVal/binary>>;
parse_selector(Value) when is_number(Value) ->
    BinVal = get_value(Value),
    <<"\\:number:", BinVal/binary>>;
parse_selector(Value) when is_boolean(Value) ->
    BinVal = get_value(Value),
    <<"\\:boolean:", BinVal/binary>>;

parse_selector(Values) when is_list(Values) ->
   lists:foldl(fun (Arg, Acc) ->
       case Arg of
           % array contains nested object
           {[{Key, Val}]} ->
                BinVal = parse_selector(Val),
                % twig:log(notice, "Come in LIST nested object Key: ~p, ParsedVal: ~p", [Key,BinVal]),
                [<<".",Key/binary,BinVal/binary>> | Acc];
            % array contains a sub array
            SubArray when is_list(SubArray) ->
                BinVals = parse_selector(SubArray),
                % twig:log(notice, "Come in LIST SubArray ~p BinVals ~p", [SubArray, BinVals]),
                lists:foldl(fun (SubArg, SubAcc) ->
                     % twig:log(notice, "Come in LIST SubArray SubArg ~p", [SubArg]),
                    [<<".\\[\\]",SubArg/binary>> | SubAcc]
                end, Acc, BinVals);
            SingleVal ->
            twig:log(notice, "Come in LIST Single Value ~p", [SingleVal]),
                [parse_selector(SingleVal) | Acc]
        end
    end,[],Values);

parse_selector({[{<<"$lt">>, Value}]}) ->
    BinVal = get_value(Value),
    Type = mango_json:type(Value),
    <<"\\:",Type/binary,":[-Infinity TO ", BinVal/binary, "}">>;
parse_selector({[{<<"$lte">>, Value}]}) ->
    BinVal = get_value(Value),
    Type = mango_json:type(Value),
    <<"\\:",Type/binary,":[-Infinity TO ", BinVal/binary, "]">>;
parse_selector({[{<<"$gt">>, Value}]}) ->
    BinVal = get_value(Value),
    Type = mango_json:type(Value),
    <<"\\:",Type/binary,":{", BinVal/binary," TO Infinity]">>;
parse_selector({[{<<"$gte">>, Value}]}) ->
    twig:log(notice, "Come in here #gte"),
    BinVal = get_value(Value),
    Type = mango_json:type(Value),
    <<"\\:", Type/binary, ":[",  BinVal/binary," TO Infinity]">>;

parse_selector({[{<<"$eq">>, Values}]}) when is_list(Values) ->
    Acc = parse_selector(Values),
    Len = list_to_binary(integer_to_list(length(Values))),
    [<<"\\:length:",Len/binary>> | Acc];
parse_selector({[{<<"$eq">>, Value}]}) ->
    parse_selector(Value);
parse_selector({[{<<"$ne">>, Value}]}) ->
    {negation, parse_selector(Value)};
parse_selector({[{<<"$not">>, Value}]}) ->
    {negation, parse_selector(Value)};
parse_selector({[{<<"$text">>, Value}]}) when is_binary(Value) ->
    parse_selector(Value);
% %% Escape the forward slashes for a regular expression
parse_selector({[{<<"$regex">>, Regex}]}) ->
    parse_selector(binary:replace(Regex, <<"/">>, <<"\/">>));
parse_selector({[{<<"$and">>, Args}]}) when is_list(Args) ->
    Values = lists:map(fun(Arg) ->
        binary_to_list(parse_selector(Arg)) end, Args),
    Values0 = list_to_binary(string:join([E || E <- Values, E /= []], " AND ")),
    <<"(",Values0/binary,")">>;
parse_selector({[{<<"$or">>, Args}]}) when is_list(Args) ->
    Values = lists:map(fun(Arg) ->
        binary_to_list(parse_selector(Arg)) end, Args),
    Values0 = list_to_binary(string:join([E || E <- Values, E /= []], " OR ")),
    <<"(",Values0/binary,")">>;
parse_selector({[{Field, {[{<<"$all">>, Args}]}}]}) when is_list(Args) ->
    Field0 = escape_lucene_chars(Field),
    Values = lists:foldl(fun (Arg, Acc) ->
       case Arg of
           % array contains nested object
           {[{Key, Val}]} ->
                BinVal = parse_selector(Val),
                Separator = get_separator(mango_json:type(Val)),
                [binary_to_list(<<Field0/binary,".",Key/binary,Separator/binary, BinVal/binary>>) | Acc];
            SingleVal ->
                BinVal = parse_selector(SingleVal),
                [binary_to_list(<<Field0/binary, BinVal/binary>>) | Acc]
        end
    end, [], Args),
    Len = list_to_binary(integer_to_list(length(Args))),
    twig:log(notice, "Values ~p, Len ~p", [Values,Len]),
    Values0 =[binary_to_list(<<Field0/binary,"\\:length:", Len/binary>>) | Values],
    Values1 = list_to_binary(string:join([E || E <- Values0, E /= []], " AND ")),
    <<"(",Values1/binary,")">>;

parse_selector({[{Field, {[{<<"$in">>, Args}]}}]}) when is_list(Args) ->
    Field0 = escape_lucene_chars(Field),
    Values = lists:foldl(fun (Arg, Acc) ->
       case Arg of
           % array contains nested object
           {[{Key, Val}]} ->
                BinVal = parse_selector(Val),
                Separator = get_separator(mango_json:type(Val)),
                [binary_to_list(<<Field0/binary,".",Key/binary,Separator/binary, BinVal/binary>>) | Acc];
            SingleVal ->
                BinVal = parse_selector(SingleVal),
                [binary_to_list(<<Field0/binary, BinVal/binary>>) | Acc]
        end
    end, [], Args),
    twig:log(notice, "Values IN ~p", [Values]),
    Values0 = list_to_binary(string:join([E || E <- Values, E /= []], " OR ")),
    <<"(",Values0/binary,")">>;
parse_selector({[{Field, {[{<<"$nin">>, Args}]}}]}) when is_list(Args) ->
    Results = parse_selector({[{Field, {[{<<"$in">>, Args}]}}]}),
    <<"(NOT ", Results/binary, ")">>;
parse_selector({[{Field, {[{<<"$elemMatch">>, {[{<<"$and">>, Queries}]}}]}}]}) ->
    twig:log(notice, "Queries ~p", [Queries]),
    Values = lists:map(fun({[{SubField,Cond}]}) ->
        twig:log(notice, "ElemMatch SubField ~p", [SubField]),
        twig:log(notice, "ElemMatch Cond~p", [Cond]),
        SubField0 = case SubField of
            <<>> -> Field;
            Else -> <<Field/binary,".",Else/binary>>
        end,
        binary_to_list(parse_selector({[{SubField0,Cond}]})) end, Queries),
    Values0 = list_to_binary(string:join([E || E <- Values, E /= []], " AND ")),
    <<"(",Values0/binary,")">>;
parse_selector({[{Field, {[{<<"$elemMatch">>, Query}]}}]}) ->
    {[{SubField,Cond}]} = Query,
    SubField0 = case SubField of
        <<>> -> Field;
        Else -> <<Field/binary,".",Else/binary>>
    end,
    Value = parse_selector({[{SubField0,Cond}]}),
    <<"(",Value/binary,")">>;
parse_selector({[{Field, {[{<<"$size">>, Arg}]}}]}) ->
    Field0 = escape_lucene_chars(Field),
    Length = get_value(Arg),
    <<Field0/binary,"\\:length:",Length/binary>>;
parse_selector({[{Field, {[{<<"$exists">>, true}]}}]}) ->
    Field0 = escape_lucene_chars(Field),
    String = <<Field0/binary,"\\:string:\/.*\/">>,
    Number = <<Field0/binary,"\\:number:[-Infinity TO Infinity]">>,
    BoolTrue = <<Field0/binary,"\\:boolean: true">>,
    BoolFalse = <<Field0/binary,"\\:boolean: false">>,
    <<"(",String/binary," OR ", Number/binary, " OR ", BoolTrue/binary, " OR ",
        BoolFalse/binary, ")">>;
%% Placeholder, not sure if $exists:false can be translated to a lucene query
parse_selector({[{Field, {[{<<"$exists">>, false}]}}]}) ->
    Value = parse_selector({[{Field, {[{<<"$exists">>, true}]}}]});
parse_selector({[{<<"default">>, Cond}]}) ->
    {[{<<"$text">>, Val}]} = Cond,
    Val0 = get_value(Val),
    case parse_selector(Cond) of
        {negation, _} ->
            <<"NOT default:",Val0/binary>>;
        _ ->
            <<"default:",Val0/binary>>
    end;
%% Object
parse_selector({[{Field, Cond}]}) ->
    Field0 = escape_lucene_chars(Field),
    twig:log(notice, "Field ~p, Cond ~p", [Field, Cond]),
    Separator = get_separator(Cond),
    case parse_selector(Cond) of
        {negation, Values} when is_list(Values) ->
            Values0 = lists:map(fun(Arg) ->
                binary_to_list(<<Field0/binary, Arg/binary>>)
            end, Values),
            RetVal = list_to_binary(string:join([E || E <- Values0, E /= []], " AND ")),
            <<"NOT (",RetVal/binary,")">>;
        Values when is_list(Values) ->
           twig:log(notice,"IsList "),
            Values0 = lists:map(fun(Arg) ->
                binary_to_list(<<Field0/binary, Arg/binary>>)
            end, Values),
            RetVal = list_to_binary(string:join([E || E <- Values0, E /= []], " AND ")),
            twig:log(notice,"RetVal Object ~p",[RetVal]),
            <<"(",RetVal/binary,")">>;
        {negation, Value} ->
            <<"NOT (", Field0/binary,Value/binary,")">>;
        Val ->
            <<Field0/binary,Separator/binary,Val/binary>>
    end.


get_value(Value) when is_binary(Value) ->
    Value;
get_value(Value) when is_integer(Value) ->
    list_to_binary(integer_to_list(Value));
get_value(Value) when is_float(Value) ->
    list_to_binary(float_to_list(Value));
get_value(Value) when is_boolean(Value) ->
    case Value of
        true -> <<"true">>;
        false -> <<"false">>
    end.


get_separator(Cond) ->
   case Cond of
        {[{<<"$gt">>, _}]} -> <<>>;
        {[{<<"$gte">>, _}]} -> <<>>;
        {[{<<"$lt">>, _}]} -> <<>>;
        {[{<<"$lte">>, _}]} -> <<>>;
        {[{<<"$eq">>, _}]} -> <<>>;
        {[{<<"$text">>, _}]} -> <<>>;
        <<"object">> -> <<".">>;
        {_} -> <<".">>;
        _ -> <<>>
    end.


%% + - && || ! ( ) { } [ ] ^ " ~ * ? : \
escape_lucene_chars(Field) when is_binary(Field) ->
    LuceneChars = [<<"+">>, <<"-">>, <<"&&">>, <<"||">>,
    <<"!">>, <<"(">>, <<")">>, <<"{">>, <<"}">>, <<"\"">>,
    <<"[">>,<<"]">>, <<"^">>, <<"\~">>, <<"*">>, <<"?">>,
    <<":">>],
    lists:foldl(fun(Char, Acc) ->
        binary:replace(Acc, Char, <<"\\", Char/binary>>)
    end, Field, LuceneChars).

% match_any_value(Field) ->
%     StringVal = <<Field/binary,"\\:string",":\/.*\/">>,
%     NumberVal = <<Field/binary,"\\:number",":[-Infinity TO Infinity]">>,
%     TrueVal =  <<Field/binary,"\\:boolean",":true">>,
%     FalseVal =  <<Field/binary,"\\:boolean",":false">>,
%     <<"(",StringVal/binary," OR ", NumberVal/binary, " OR ", TrueVal/binary,
%         " OR ", FalseVal/binary, ")">>.


%% Normalize Text Selector
%% This is more like a validator than a normalizer since we except users to provide
%% a text search in the following format:
%% {
%%    "$text": query-string,
%%    "$options":
%%      {
%%       "$bookmark":  val1,
%%       "$counts":    val2,
%%       "$ranges": val3
%%      }
%% }
%% Otherwise, we throw an error.
%% In the future, we can accept more complex sytnax that we can normalize.
% normalize({[]}) ->
%     {[]};
% normalize(Selector) ->
%     Steps = [
%         fun norm_ops/1
%     ],
%     {NProps} = lists:foldl(fun(Step, Sel) -> Step(Sel) end, Selector, Steps),
%     {NProps}.

% %% text seach operators
% norm_ops({[{<<"$text">>, Arg}]}) when is_binary(Arg); is_number(Arg); is_boolean(Arg) ->
%     {[{<<"$text">>, Arg}]};
% norm_ops({[{<<"$text">>, Arg}]}) ->
%     ?MANGO_ERROR({bad_arg, '$text', Arg});

% %% Options with $text
% norm_ops({[{<<"$text">>, Arg}, Opts]}) when is_binary(Arg); is_number(Arg); is_boolean(Arg) ->
%      {[{<<"$text">>, Arg}, norm_ops(Opts)]};
% norm_ops({[{<<"$text">>, Arg}, _]}) ->
%       ?MANGO_ERROR({bad_arg, '$text', Arg});


% norm_ops({<<"$options">>, {Args}}) ->
%     Opts = lists:map(fun(Arg) -> norm_ops(Arg) end, Args),
%     {<<"$options">>, {Opts}};

% norm_ops({<<"$bookmark">>, Arg}) when is_binary(Arg) ->
%     {<<"$bookmark">>, Arg};
% norm_ops({[{<<"$bookmark">>, Arg}]}) ->
%     ?MANGO_ERROR({bad_arg, '$bookmark', Arg});
% norm_ops({<<"$counts">>, Arg}) when is_list(Arg) ->
%     {<<"$counts">>, Arg};
% norm_ops({[{<<"$counts">>, Arg}]}) ->
%     ?MANGO_ERROR({bad_arg, '$counts', Arg});
% norm_ops({<<"$ranges">>, {_}=Arg}) ->
%     {<<"$ranges">>, Arg};
% norm_ops({<<"$ranges">>, _}) ->
%     ?MANGO_ERROR({bad_arg, '$ranges'});


% % Known but unsupported text operators
% norm_ops({<<"$group">>, _}) ->
%     ?MANGO_ERROR({not_supported, '$group'});
% % Unknown operator
% norm_ops({<<"$", _/binary>>=Op, _}) ->
%     ?MANGO_ERROR({invalid_operator, Op});
% norm_ops({[{<<"$", _/binary>>=Op, _}]}) ->
%     ?MANGO_ERROR({invalid_operator, Op});


% norm_ops(Arg) ->
%     ?MANGO_ERROR({invalid_selector, Arg}).
