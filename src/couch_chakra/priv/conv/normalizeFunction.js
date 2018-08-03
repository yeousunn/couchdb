function normalizeFunction(fun) {
  var ast = esprima.parse(fun, {tolerant: true});
  var idx = ast.body.length - 1;
  var fun_decl = {};

  // Search for the first FunctionDeclaration beginning from the end
  do {
    fun_decl = ast.body[idx--];
  } while(idx >= 0 && fun_decl.type != "FunctionDeclaration")
  idx++;

  // If we have a function declaration with an Id, wrap it
  // in an ExpressionStatement and change it into
  // a FuntionExpression
	if(fun_decl.type == "FunctionDeclaration" && fun_decl.id == null){
    fun_decl.type = "FunctionExpression";
    ast.body[idx] = {
       "type": "ExpressionStatement",
       "expression": fun_decl
    };
	}

  // Re-generate the rewritten AST
	return escodegen.generate(ast);
}
