
var _config = {};
function set_query_config(config) {
  _config = config;
}

function runReduce(reduceFuns, keys, values, rereduce) {
  var code_size = 0;
  for (var i in reduceFuns) {
    var fun_body =  reduceFuns[i];
    code_size += fun_body.length;
    reduceFuns[i] = Couch.compileFunction(fun_body);
  };
  var reductions = new Array(reduceFuns.length);
  for(var i = 0; i < reduceFuns.length; i++) {
    try {
      reductions[i] = reduceFuns[i](keys, values, rereduce);
    } catch (err) {
      handleViewError(err);
      // if the error is not fatal, ignore the results and continue
      reductions[i] = null;
    }
  };
  var input_length = JSON.stringify([keys, values]).length;
  var reduce_line = JSON.stringify(reductions);
  var reduce_length = reduce_line.length;
  // TODO make reduce_limit config into a number
  if (_config && _config.reduce_limit &&
        reduce_length > 4096 && ((reduce_length * 2) > input_length)) {
    var log_message = [
        "Reduce output must shrink more rapidly:",
        "input size:", input_length,
        "output size:", reduce_length
    ].join(" ");
    if (_config.reduce_limit === "log") {
        log("reduce_overflow_error: " + log_message);
        print("[true," + reduce_line + "]");
    } else {
        throw(["error", "reduce_overflow_error", log_message]);
    };
  } else {
    return [_log_messages, reductions];
  }
};


function reduce(reduceFuns, kvs) {
  clear_logs();
  var keys = new Array(kvs.length);
  var values = new Array(kvs.length);
  for(var i = 0; i < kvs.length; i++) {
      keys[i] = kvs[i][0];
      values[i] = kvs[i][1];
  }
  return runReduce(reduceFuns, keys, values, false);
};


function rereduce(reduceFuns, values) {
  clear_logs();
  return runReduce(reduceFuns, null, values, true);
};
