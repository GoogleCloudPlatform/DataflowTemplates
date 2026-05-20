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

"""Unit tests for bqmonitor.safe_eval."""

import logging
import unittest

logging.basicConfig(level=logging.INFO)

from bqmonitor.safe_eval import Expr


class ExprArithmeticTest(unittest.TestCase):
  """Tests for arithmetic operations."""

  def test_addition(self):
    self.assertEqual(Expr('a + b')({'a': 3, 'b': 4}), 7)

  def test_subtraction(self):
    self.assertEqual(Expr('a - b')({'a': 10, 'b': 3}), 7)

  def test_multiplication(self):
    self.assertEqual(Expr('a * b')({'a': 5, 'b': 6}), 30)

  def test_division(self):
    self.assertAlmostEqual(Expr('a / b')({'a': 10, 'b': 3}), 10 / 3)

  def test_floor_division(self):
    self.assertEqual(Expr('a // b')({'a': 10, 'b': 3}), 3)

  def test_modulo(self):
    self.assertEqual(Expr('a % b')({'a': 10, 'b': 3}), 1)

  def test_power(self):
    self.assertEqual(Expr('x ** 2')({'x': 5}), 25)

  def test_negation(self):
    self.assertEqual(Expr('-x')({'x': 7}), -7)

  def test_parentheses(self):
    self.assertEqual(Expr('(a + b) * c')({'a': 2, 'b': 3, 'c': 4}), 20)


class ExprComparisonTest(unittest.TestCase):
  """Tests for comparison operations."""

  def test_eq(self):
    self.assertTrue(Expr('x == 1')({'x': 1}))
    self.assertFalse(Expr('x == 1')({'x': 2}))

  def test_neq(self):
    self.assertTrue(Expr('x != 1')({'x': 2}))

  def test_lt(self):
    self.assertTrue(Expr('x < 5')({'x': 3}))
    self.assertFalse(Expr('x < 5')({'x': 5}))

  def test_lte(self):
    self.assertTrue(Expr('x <= 5')({'x': 5}))

  def test_gt(self):
    self.assertTrue(Expr('x > 5')({'x': 6}))

  def test_gte(self):
    self.assertTrue(Expr('x >= 5')({'x': 5}))

  def test_string_comparison(self):
    self.assertTrue(Expr("s == 'ok'")({'s': 'ok'}))
    self.assertFalse(Expr("s == 'ok'")({'s': 'fail'}))


class ExprBooleanTest(unittest.TestCase):
  """Tests for boolean logic."""

  def test_and(self):
    self.assertTrue(Expr('a > 0 and b > 0')({'a': 1, 'b': 1}))
    self.assertFalse(Expr('a > 0 and b > 0')({'a': 1, 'b': -1}))

  def test_or(self):
    self.assertTrue(Expr('a > 0 or b > 0')({'a': -1, 'b': 1}))
    self.assertFalse(Expr('a > 0 or b > 0')({'a': -1, 'b': -1}))

  def test_not(self):
    self.assertTrue(Expr('not failed')({'failed': False}))
    self.assertFalse(Expr('not failed')({'failed': True}))

  def test_compound(self):
    e = Expr("x > 0 and not disabled or override == 'yes'")
    self.assertTrue(e({'x': 1, 'disabled': False, 'override': 'no'}))
    self.assertTrue(e({'x': -1, 'disabled': True, 'override': 'yes'}))
    self.assertFalse(e({'x': -1, 'disabled': True, 'override': 'no'}))


class ExprIfElseTest(unittest.TestCase):
  """Tests for conditional expressions."""

  def test_if_else(self):
    e = Expr("1 if status == 'ok' else 0")
    self.assertEqual(e({'status': 'ok'}), 1)
    self.assertEqual(e({'status': 'fail'}), 0)

  def test_if_else_with_bool(self):
    e = Expr("1 if a > 0 and b > 0 else 0")
    self.assertEqual(e({'a': 1, 'b': 1}), 1)
    self.assertEqual(e({'a': 1, 'b': -1}), 0)


class ExprBuiltinsTest(unittest.TestCase):
  """Tests for safe builtin functions."""

  def test_abs(self):
    self.assertEqual(Expr('abs(x)')({'x': -7}), 7)
    self.assertEqual(Expr('abs(x)')({'x': 7}), 7)

  def test_min(self):
    self.assertEqual(Expr('min(a, b)')({'a': 3, 'b': 5}), 3)

  def test_max(self):
    self.assertEqual(Expr('max(a, b)')({'a': 3, 'b': 5}), 5)

  def test_max_with_literal(self):
    self.assertEqual(Expr('max(x, 1)')({'x': 0}), 1)

  def test_round(self):
    self.assertAlmostEqual(Expr('round(x, 2)')({'x': 3.14159}), 3.14)

  def test_nested_builtins(self):
    self.assertEqual(Expr('max(abs(a), abs(b))')({'a': -5, 'b': 3}), 5)


class ExprFieldRefsTest(unittest.TestCase):
  """Tests for field_refs() extraction."""

  def test_simple(self):
    self.assertEqual(Expr('a + b').field_refs(), {'a', 'b'})

  def test_excludes_builtins(self):
    self.assertEqual(
        Expr('max(clicks, 1) / abs(total)').field_refs(),
        {'clicks', 'total'})

  def test_if_else_refs(self):
    self.assertEqual(
        Expr("1 if status == 'ok' else 0").field_refs(), {'status'})

  def test_no_refs(self):
    self.assertEqual(Expr('1 + 2').field_refs(), set())


class ExprValidationTest(unittest.TestCase):
  """Tests for expression validation / rejection."""

  def test_rejects_import(self):
    with self.assertRaises((ValueError, SyntaxError)):
      Expr('__import__("os")')

  def test_rejects_unknown_function(self):
    with self.assertRaises(ValueError) as ctx:
      Expr('eval("bad")')
    self.assertIn('eval', str(ctx.exception))

  def test_rejects_attribute_access(self):
    with self.assertRaises(ValueError):
      Expr('x.__class__')

  def test_rejects_subscript(self):
    with self.assertRaises(ValueError):
      Expr('x[0]')

  def test_rejects_lambda(self):
    with self.assertRaises((ValueError, SyntaxError)):
      Expr('lambda x: x')

  def test_rejects_chained_comparison(self):
    with self.assertRaises(ValueError) as ctx:
      Expr('a < b < c')
    self.assertIn('Chained', str(ctx.exception))

  def test_rejects_keyword_args(self):
    with self.assertRaises(ValueError) as ctx:
      Expr('round(x, ndigits=2)')
    self.assertIn('Keyword', str(ctx.exception))

  def test_rejects_unsupported_literal(self):
    with self.assertRaises(ValueError) as ctx:
      Expr('b"bytes"')
    self.assertIn('bytes', str(ctx.exception))


class ExprPickleTest(unittest.TestCase):
  """Tests for pickle/unpickle support."""

  def test_roundtrip(self):
    import pickle
    original = Expr('a + b')
    restored = pickle.loads(pickle.dumps(original))
    self.assertEqual(original, restored)
    self.assertEqual(restored({'a': 1, 'b': 2}), 3)


class ExprRealWorldTest(unittest.TestCase):
  """Tests using realistic metric expressions."""

  def test_ctr(self):
    e = Expr('clicks / impressions')
    self.assertAlmostEqual(e({'clicks': 50, 'impressions': 1000}), 0.05)

  def test_safe_ctr(self):
    e = Expr('clicks / max(impressions, 1)')
    self.assertEqual(e({'clicks': 0, 'impressions': 0}), 0.0)

  def test_derived_field(self):
    e = Expr("1 if status == 'success' else 0")
    self.assertEqual(e({'status': 'success'}), 1)
    self.assertEqual(e({'status': 'error'}), 0)

  def test_threshold_expression(self):
    e = Expr('value >= 100')
    self.assertTrue(e({'value': 500}))
    self.assertFalse(e({'value': 50}))

  def test_range_threshold(self):
    e = Expr('value > 100 or value < -100')
    self.assertTrue(e({'value': 200}))
    self.assertTrue(e({'value': -200}))
    self.assertFalse(e({'value': 50}))


if __name__ == '__main__':
  unittest.main()
