
function isArray(obj) {
  return toString.call(obj) === "[object Array]";
}


var _log_messages = [];
function log(message) {
  if (typeof message != "string") {
    message = JSON.stringify(message);
  }
  _log_messages.push(String(message));
  if(_log_messages.length > 10) {
    _log_messages.shift()
  }
}


function clear_log() {
  _log_messages = [];
}


var resolveModule = function(names, mod, root) {
  if (names.length == 0) {
    if (typeof mod.current != "string") {
      throw ["error","invalid_require_path",
        'Must require a JavaScript string, not: '+(typeof mod.current)];
    }
    return {
      current : mod.current,
      parent : mod.parent,
      id : mod.id,
      exports : {}
    };
  }
  // we need to traverse the path
  var n = names.shift();
  if (n == '..') {
    if (!(mod.parent && mod.parent.parent)) {
      throw ["error", "invalid_require_path", 'Object has no parent '+JSON.stringify(mod.current)];
    }
    return resolveModule(names, {
      id : mod.id.slice(0, mod.id.lastIndexOf('/')),
      parent : mod.parent.parent,
      current : mod.parent.current
    });
  } else if (n == '.') {
    if (!mod.parent) {
      throw ["error", "invalid_require_path", 'Object has no parent '+JSON.stringify(mod.current)];
    }
    return resolveModule(names, {
      parent : mod.parent,
      current : mod.current,
      id : mod.id
    });
  } else if (root) {
    mod = {current : root};
  }
  if (mod.current[n] === undefined) {
    throw ["error", "invalid_require_path", 'Object has no property "'+n+'". '+JSON.stringify(mod.current)];
  }
  return resolveModule(names, {
    current : mod.current[n],
    parent : mod,
    id : mod.id ? mod.id + '/' + n : n
  });
};


var Couch = {
  // moving this away from global so we can move to json2.js later
  compileFunction : function(source, ddoc) {
    if (!source) throw(["error","not_found","missing function"]);

    var functionObject = null;

    var require = function(name, module) {
      module = module || {};
      var newModule = resolveModule(name.split('/'), module.parent, ddoc);
      if (!ddoc._module_cache.hasOwnProperty(newModule.id)) {
        // create empty exports object before executing the module,
        // stops circular requires from filling the stack
        ddoc._module_cache[newModule.id] = {};
        var s = "function (module, exports, require) { " + newModule.current + "\n }";
        try {
          var func = sandbox ? eval(s) : eval(s);
          func.apply(sandbox, [newModule, newModule.exports, function(name) {
            return require(name, newModule);
          }]);
        } catch(e) {
          throw [
            "error",
            "compilation_error",
            "Module require('" +name+ "') raised error " +
            (e.toSource ? e.toSource() : e.stack)
          ];
        }
        ddoc._module_cache[newModule.id] = newModule.exports;
      }
      return ddoc._module_cache[newModule.id];
    };

    if (ddoc) {
      if (!ddoc._module_cache) ddoc._module_cache = {};
    }

    try {
      functionObject = eval(source);
    } catch (err) {
      throw([
        "error",
        "compilation_error",
        (err.toSource ? err.toSource() : err.stack) + " (" + source + ")"
      ]);
    };
    if (typeof(functionObject) == "function") {
      return functionObject;
    } else {
      throw(["error","compilation_error",
        "Expression does not eval to a function. (" + source.toString() + ")"]);
    };
  }
};


function handleViewError(err, doc) {
  if (err == "fatal_error") {
    // Only if it's a "fatal_error" do we exit. What's a fatal error?
    // That's for the query to decide.
    //
    // This will make it possible for queries to completely error out,
    // by catching their own local exception and rethrowing a
    // fatal_error. But by default if they don't do error handling we
    // just eat the exception and carry on.
    //
    // In this case we abort map processing but don't destroy the
    // JavaScript process. If you need to destroy the JavaScript
    // process, throw the error form matched by the block below.
    throw(["error", "map_runtime_error", "function raised 'fatal_error'"]);
  } else if (err[0] == "fatal") {
    // Throwing errors of the form ["fatal","error_key","reason"]
    // will kill the OS process. This is not normally what you want.
    throw(err);
  }
  var message = "function raised exception " +
                (err.toSource ? err.toSource() : err.stack);
  if (doc) message += " with doc._id " + doc._id;
  log(message);
};
