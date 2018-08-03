
main([]) ->
  %Prompt = io_lib:format("GDB Attach to: ~s~n", [os:getpid()]),
  %io:get_line(Prompt),
  %Script = <<"function(doc, req) {\n log(\"ok\");\n    return [doc.title, doc.body].join(' - ');\n }\n">>,
  Script = <<"function(doc, req) {\n return;\n }\n">>,
  {ok, Pid} = couch_chakra:start_link(),
  couch_chakra:prompt(Pid, [<<"reset">>]),
  couch_chakra:prompt(Pid, [<<"add_fun">>, Script]).
