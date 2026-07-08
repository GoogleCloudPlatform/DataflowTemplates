#
# Copyright (C) 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

"""Safe expression evaluator for metric computation.

Parses a Python expression string, validates it uses only allowed
constructs, compiles it, and produces a callable that evaluates the
expression against a field context dict.

Example usage::

  from bqmonitor.safe_eval import Expr

  expr = Expr.from_string("clicks / impressions")
  expr({'clicks': 50, 'impressions': 1000})   # 0.05

  expr = Expr.from_string("1 if status == 'success' else 0")
  expr({'status': 'success'})                  # 1

Allowed constructs: field names (bare names), literals (int, float, str),
arithmetic (``+, -, *, /, //, %, **``), comparisons (``==, !=, <, <=, >, >=``),
boolean logic (``and, or, not``), unary negation, ``if/else``, and
safe builtins (``abs, min, max, round``).
"""

import ast

# --- AST whitelist ---

_ALLOWED_BINOPS = (
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow)

_ALLOWED_CMPOPS = (ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE)

_SAFE_BUILTINS = {"abs": abs, "min": min, "max": max, "round": round}


def _validate_ast(node):
  """Recursively validate that an AST node uses only allowed constructs."""
  if isinstance(node, ast.Name):
    return

  if isinstance(node, ast.Constant):
    if not isinstance(node.value, (int, float, str)):
      raise ValueError(
          f"Unsupported literal type: {type(node.value).__name__}. "
          f"Only int, float, and str literals are supported.")
    return

  if isinstance(node, ast.UnaryOp):
    if not isinstance(node.op, (ast.USub, ast.Not)):
      raise ValueError(
          f"Unsupported unary operator: {type(node.op).__name__}. "
          f"Only negation (-) and not are supported.")
    _validate_ast(node.operand)
    return

  if isinstance(node, ast.BinOp):
    if not isinstance(node.op, _ALLOWED_BINOPS):
      raise ValueError(
          f"Unsupported binary operator: {type(node.op).__name__}. "
          f"Supported: +, -, *, /, //, %, **")
    _validate_ast(node.left)
    _validate_ast(node.right)
    return

  if isinstance(node, ast.BoolOp):
    # and / or
    for value in node.values:
      _validate_ast(value)
    return

  if isinstance(node, ast.Compare):
    if len(node.ops) != 1 or len(node.comparators) != 1:
      raise ValueError(
          "Chained comparisons not supported (e.g., a < b < c). "
          "Use (a < b) and separate expressions instead.")
    if not isinstance(node.ops[0], _ALLOWED_CMPOPS):
      raise ValueError(
          f"Unsupported comparison: {type(node.ops[0]).__name__}. "
          f"Supported: ==, !=, <, <=, >, >=")
    _validate_ast(node.left)
    _validate_ast(node.comparators[0])
    return

  if isinstance(node, ast.IfExp):
    _validate_ast(node.test)
    _validate_ast(node.body)
    _validate_ast(node.orelse)
    return

  if isinstance(node, ast.Call):
    if not (isinstance(node.func, ast.Name)
            and node.func.id in _SAFE_BUILTINS):
      name = node.func.id if isinstance(node.func, ast.Name) else ast.dump(
          node.func)
      raise ValueError(
          f"Unsupported function: {name}. "
          f"Supported: {', '.join(sorted(_SAFE_BUILTINS))}.")
    if node.keywords:
      raise ValueError("Keyword arguments not supported in function calls.")
    for arg in node.args:
      _validate_ast(arg)
    return

  raise ValueError(
      f"Unsupported expression: {ast.dump(node)}. "
      f"Only field names, literals, arithmetic (+,-,*,/,//,%,**), "
      f"comparisons (==,!=,<,<=,>,>=), boolean logic (and, or, not), "
      f"if/else, and functions ({', '.join(sorted(_SAFE_BUILTINS))}) "
      f"are supported.")


def _collect_field_refs(node):
  """Collect all field names referenced in an AST (excluding builtins)."""
  return frozenset(
      child.id for child in ast.walk(node)
      if isinstance(child, ast.Name) and child.id not in _SAFE_BUILTINS)


class Expr:
  """A validated, compiled expression callable.

  Parses a Python expression string, validates it uses only safe
  constructs, and compiles it into a callable. The compiled expression
  is evaluated with restricted builtins (no access to ``import``,
  ``open``, ``exec``, etc.).

  Args:
    text: A Python expression string.

  Raises:
    ValueError: If the expression uses unsupported Python constructs.
    SyntaxError: If the string is not valid Python syntax.
  """
  def __init__(self, text):
    self._text = text
    tree = ast.parse(text, mode='eval')
    _validate_ast(tree.body)
    self._code = compile(tree, '<expr>', 'eval')
    self._refs = _collect_field_refs(tree.body)

  def __call__(self, context):
    """Evaluate the expression against a dict of field values."""
    env = dict(_SAFE_BUILTINS)
    env.update(context)
    return eval(self._code, {"__builtins__": {}}, env)

  def field_refs(self):
    """Return the set of field names referenced by this expression."""
    return set(self._refs)

  @staticmethod
  def from_string(text):
    """Parse a Python expression string into a compiled Expr callable.

    Examples::

      Expr.from_string("clicks / impressions")
      Expr.from_string("1 if status == 'success' else 0")
      Expr.from_string("(a + b) / total")
    """
    return Expr(text)

  def __str__(self):
    return self._text

  def __repr__(self):
    return f"Expr({self._text!r})"

  def __eq__(self, other):
    return isinstance(other, Expr) and self._text == other._text

  def __hash__(self):
    return hash(self._text)

  def __reduce__(self):
    """Pickle support: store text, recompile on unpickle."""
    return (Expr, (self._text, ))
