var _ddoc = null;

function set_ddoc(ddoc) {
  _ddoc = ddoc
};


function _run_vdu(fun, ddoc, args) {
  try {
    fun.apply(ddoc, args);
    return 1;
  } catch (error) {
    if (error.name && error.stack) {
      throw error;
    }
    return error;
  }
};


function _run_show(fun, ddoc, args) {
  return false;
}


function _run_list(fun, ddoc, args) {
  return false;
}


function _run_filter(fun, ddoc, args) {
  return false;
}


function _run_filter_view(fun, ddoc, args) {
  return false;
}


function _run_update(fun, ddoc, args) {
  return false;
}


function _run_rewrite(fun, ddoc, args) {
  return false;
}


var _dispatch = {
  "validate_doc_update": _run_vdu,
  "shows": _run_show,
  "lists": _run_list,
  "filters": _run_filter,
  "views": _run_filter_view,
  "updates": _run_update,
  "rewrites": _run_rewrite
};


function get_source(ddocId, funPath) {
  // the first member of the fun path determines the type of operation
  var cmd = funPath[0];
  if (_dispatch[cmd]) {
    // get the function, call the command with it
    var point = _ddoc;
    for (var i=0; i < funPath.length; i++) {
      if (i+1 == funPath.length) {
        var fun = point[funPath[i]];

        if (!fun) {
          throw(["error","not_found",
                 "missing " + funPath[0] + " function " + funPath[i] +
                 " on design doc " + ddocId]);
        }

        if (typeof fun != "function") {
          return fun;
        };

        return false;
      } else {
        point = point[funPath[i]];
      }
    };
  } else {
    // unknown command, quit and hope the restarted version is better
    throw(["fatal", "unknown_command", "unknown ddoc command '" + cmd + "'"]);
  }
}


function run(ddocId, funPath, funArgs) {
  // the first member of the fun path determines the type of operation
  var cmd = funPath[0];
  if (_dispatch[cmd]) {
    // get the function, call the command with it
    var point = _ddoc;
    for (var i=0; i < funPath.length; i++) {
      if (i+1 == funPath.length) {
        var fun = point[funPath[i]];
        if (!fun) {
          throw(["error","not_found",
                 "missing " + funPath[0] + " function " + funPath[i] +
                 " on design doc " + ddocId]);
        }
        if (typeof fun != "function") {
          fun = normalizeFunction(fun);
          fun = Couch.compileFunction(fun, _ddoc);
          // cache the compiled fun on the ddoc
          point[funPath[i]] = fun;
        };
      } else {
        point = point[funPath[i]];
      }
    };

    // run the correct responder with the cmd body
    _dispatch[cmd].apply(null, [fun, _ddoc, funArgs]);
  } else {
    // unknown command, quit and hope the restarted version is better
    throw(["fatal", "unknown_command", "unknown ddoc command '" + cmd + "'"]);
  }
};
