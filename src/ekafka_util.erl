%%%-------------------------------------------------------------------
%%% @author luyou
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. 十二月 2015 11:14
%%%-------------------------------------------------------------------
-module(ekafka_util).
-author("luyou").

-include("ekafka.hrl").

%% API
-compile(export_all).


%% to_atom/1
%% ====================================================================
%% @doc : covert data to atom
%%@return: atom
to_atom(undefined) ->
    undefined;
to_atom([]) ->
    undefined;
to_atom(Data) when erlang:is_number(Data) ->
    to_atom(erlang:integer_to_list(erlang:trunc(Data)));
to_atom(Data) when erlang:is_binary(Data) ->
    erlang:binary_to_atom(Data, utf8);
to_atom(Data) when erlang:is_list(Data) ->
    case erlang:length(Data) of
        L when L > 255 ->
            undefined;
        _ ->
            erlang:list_to_atom(Data)
    end;
to_atom(Data) ->
    Data.


%% to_list/1
%% ====================================================================
%% @doc : covert data to list
%%@return: list
to_list(undefined) ->
    "";
to_list([]) ->
    "";
to_list(Data) when erlang:is_number(Data) ->
    erlang:integer_to_list(erlang:trunc(Data));
to_list(Data) when erlang:is_atom(Data) ->
    erlang:atom_to_list(Data);
to_list(Data) when erlang:is_binary(Data) ->
    erlang:binary_to_list(Data);
to_list(Data) when erlang:is_pid(Data) ->
    erlang:pid_to_list(Data);
to_list(Data) when erlang:is_tuple(Data) ->
    erlang:tuple_to_list(Data);
to_list(Data) ->
    Data.

%% to_binary/1
%% ====================================================================
%% @doc : covert data to list
%%@return: list
to_binary(undefined) ->
    <<>>;
to_binary([]) ->
    <<>>;
to_binary(Data) when erlang:is_integer(Data) ->
    erlang:list_to_binary(erlang:integer_to_list(Data));
to_binary(Data) when erlang:is_number(Data) ->
    erlang:integer_to_binary(erlang:trunc(Data));
to_binary(Data) when erlang:is_list(Data) ->
    erlang:list_to_binary(Data);
to_binary(Data) when erlang:is_atom(Data) ->
    erlang:atom_to_binary(Data, utf8);
to_binary(Data) when erlang:is_tuple(Data) ->
    erlang:list_to_binary(erlang:tuple_to_list(Data));
to_binary(Data) ->
    Data.


%% to_integer/1
%% ====================================================================
%% @doc : covert data to list
%%@return: list
to_integer(undefined) ->
    0;
to_integer(<<"undefined">>) ->
    0;
to_integer("undefined") ->
    0;
to_integer(<<"undefine">>) ->
    0;
to_integer("undefine") ->
    0;
to_integer(<<>>) ->
    0;
to_integer([]) ->
    0;
to_integer(Data) when erlang:is_integer(Data) ->
    Data;
to_integer(Data) ->
    try
        erlang:list_to_integer(to_list(Data))
    catch
        _Error ->
            0;
        exit:_Reason ->
            0;
        error:_Reason ->
            0;
        _Class:_Reason ->
            0
    end.


%% to_ip4_address/1
%% ====================================================================
%% @doc : covert data to ip4_address {0..255,0..255,0..255}
%%@return: ip4_address
to_ip4_address(Data) when erlang:is_list(Data) ->
    to_ip4_address(to_binary(Data));
to_ip4_address(Data) when erlang:is_binary(Data) ->
    IntList =
        lists:foldr(fun(Byte, L) ->
            [to_integer(Byte) | L]
        end, [], binary:split(Data, <<".">>, [global, trim_all])),
    erlang:list_to_tuple(IntList).


%% set_conf/2
%% ====================================================================
%% @doc : set conf
%% @return: void
set_conf(Key, Value) ->
    Row = #ekafka_conf{key = Key, value = Value},
    ets:insert(?EKAFKA_CONF, Row).

%% get_conf/1
%% ====================================================================
%% @doc : retrieve conf
%% @return: value
get_conf(Key) ->
    case ets:lookup(?EKAFKA_CONF, Key) of
        [ConfData] ->
            #ekafka_conf{value = Value} = ConfData,
            Value;
        _Any ->
            undefined
    end.

get_max_workers(Role) ->
    case Role of
        producer ->
            case get_conf(max_workers) of
                undefined -> ?MAX_PRODUCER_PROCESSES;
                Value     -> Value
            end;
        _ ->
            1
    end.
