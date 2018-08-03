
var _map_results = null;

function emit(key, value) {
  _map_results.push([key, value]);
};


function sum(values) {
  var rv = 0;
  for (var i in values) {
    rv += values[i];
  }
  return rv;
};

var _lib = null;
function add_lib(lib) {
  _lib = lib;
}

var _functions = [];
function add_fun(script) {
  _functions.push(Couch.compileFunction(script, {views : {lib : _lib}}))
}


function map_doc(doc) {
  // Compute all the map functions against the document.
  //
  // Each function can output multiple key/value pairs for each document.
  //
  // Example output of map_doc after three functions set by add_fun cmds:
  // [
  //  [["Key","Value"]],                    <- fun 1 returned 1 key value
  //  [],                                   <- fun 2 returned 0 key values
  //  [["Key1","Value1"],["Key2","Value2"]] <- fun 3 returned 2 key values
  // ]
  //
  clear_log();

  var buf = [];

  for (fun in _functions) {
    _map_results = [];

    try {
      fun(doc);
      buf.push(map_results);
    } catch (err) {
      handleViewError(err, doc);
      // If the error is not fatal, we treat the doc as if it
      // did not emit anything, by buffering an empty array.
      buf.push([]);
    }
  }

  return [_log_mesages, buf];
};
