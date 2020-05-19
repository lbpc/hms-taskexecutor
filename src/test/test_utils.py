import unittest
from collections import namedtuple
from types import SimpleNamespace

from taskexecutor.utils import attrs_to_env


class TestAttrsToEnv(unittest.TestCase):
    def test_simple_object(self):
        o = SimpleNamespace(number=1, string='spam', snake_case='hisss', camelCase='???')
        self.assertEqual(
            attrs_to_env(o),
            {'$number': '1',
             '$NUMBER': '1',
             '$string': 'spam',
             '$STRING': 'spam',
             '$snake_case': 'hisss',
             '$SNAKE_CASE': 'hisss',
             '$snake-case': 'hisss',
             '$SNAKE-CASE': 'hisss',
             '$camelCase': '???',
             '$CAMELCASE': '???',
             '$camel_case': '???',
             '$CAMEL_CASE': '???',
             '$camel-case': '???',
             '$CAMEL-CASE': '???',
             '${number}': '1',
             '${NUMBER}': '1',
             '${string}': 'spam',
             '${STRING}': 'spam',
             '${snake_case}': 'hisss',
             '${SNAKE_CASE}': 'hisss',
             '${snake-case}': 'hisss',
             '${SNAKE-CASE}': 'hisss',
             '${camelCase}': '???',
             '${CAMELCASE}': '???',
             '${camel_case}': '???',
             '${CAMEL_CASE}': '???',
             '${camel-case}': '???',
             '${CAMEL-CASE}': '???'}
        )

    def test_no_sigils(self):
        o = SimpleNamespace(a=1)
        self.assertEqual(attrs_to_env(o, sigils=False), {'a': '1', 'A': '1', '{a}': '1', '{A}': '1'})

    def test_no_brackets(self):
        o = SimpleNamespace(a=1)
        self.assertEqual(attrs_to_env(o, brackets=False), {'$a': '1', '$A': '1'})

    def test_no_sigils_no_brackets(self):
        o = SimpleNamespace(a=1)
        self.assertEqual(attrs_to_env(o, sigils=False, brackets=False), {'a': '1', 'A': '1'})

    def test_nested_object(self):
        o = SimpleNamespace(spam=SimpleNamespace(eggs=SimpleNamespace(bacon=1, sausage=2),
                                                 brandy=3))
        self.assertEqual(attrs_to_env(o),
                         {'$spam_eggs_bacon': '1',
                          '$spam_eggs_BACON': '1',
                          '$spam_EGGS_bacon': '1',
                          '$spam_EGGS_BACON': '1',
                          '$SPAM_eggs_bacon': '1',
                          '$SPAM_eggs_BACON': '1',
                          '$SPAM_EGGS_bacon': '1',
                          '$SPAM_EGGS_BACON': '1',
                          '$spam_eggs_sausage': '2',
                          '$spam_eggs_SAUSAGE': '2',
                          '$spam_EGGS_sausage': '2',
                          '$spam_EGGS_SAUSAGE': '2',
                          '$SPAM_eggs_sausage': '2',
                          '$SPAM_eggs_SAUSAGE': '2',
                          '$SPAM_EGGS_sausage': '2',
                          '$SPAM_EGGS_SAUSAGE': '2',
                          '$spam_brandy': '3',
                          '$spam_BRANDY': '3',
                          '$SPAM_brandy': '3',
                          '$SPAM_BRANDY': '3',
                          '${spam_eggs_bacon}': '1',
                          '${spam_eggs_BACON}': '1',
                          '${spam_EGGS_bacon}': '1',
                          '${spam_EGGS_BACON}': '1',
                          '${SPAM_eggs_bacon}': '1',
                          '${SPAM_eggs_BACON}': '1',
                          '${SPAM_EGGS_bacon}': '1',
                          '${SPAM_EGGS_BACON}': '1',
                          '${spam_eggs_sausage}': '2',
                          '${spam_eggs_SAUSAGE}': '2',
                          '${spam_EGGS_sausage}': '2',
                          '${spam_EGGS_SAUSAGE}': '2',
                          '${SPAM_eggs_sausage}': '2',
                          '${SPAM_eggs_SAUSAGE}': '2',
                          '${SPAM_EGGS_sausage}': '2',
                          '${SPAM_EGGS_SAUSAGE}': '2',
                          '${spam_brandy}': '3',
                          '${spam_BRANDY}': '3',
                          '${SPAM_brandy}': '3',
                          '${SPAM_BRANDY}': '3'})

    def test_dict(self):
        self.assertEqual(attrs_to_env(
            {'spam': {'eggs': {'bacon': 1,
                               'sausage': 2},
                      'brandy': 3}}),
            {'$spam_eggs_bacon': '1',
             '$spam_eggs_BACON': '1',
             '$spam_EGGS_bacon': '1',
             '$spam_EGGS_BACON': '1',
             '$SPAM_eggs_bacon': '1',
             '$SPAM_eggs_BACON': '1',
             '$SPAM_EGGS_bacon': '1',
             '$SPAM_EGGS_BACON': '1',
             '$spam_eggs_sausage': '2',
             '$spam_eggs_SAUSAGE': '2',
             '$spam_EGGS_sausage': '2',
             '$spam_EGGS_SAUSAGE': '2',
             '$SPAM_eggs_sausage': '2',
             '$SPAM_eggs_SAUSAGE': '2',
             '$SPAM_EGGS_sausage': '2',
             '$SPAM_EGGS_SAUSAGE': '2',
             '$spam_brandy': '3',
             '$spam_BRANDY': '3',
             '$SPAM_brandy': '3',
             '$SPAM_BRANDY': '3',
             '${spam_eggs_bacon}': '1',
             '${spam_eggs_BACON}': '1',
             '${spam_EGGS_bacon}': '1',
             '${spam_EGGS_BACON}': '1',
             '${SPAM_eggs_bacon}': '1',
             '${SPAM_eggs_BACON}': '1',
             '${SPAM_EGGS_bacon}': '1',
             '${SPAM_EGGS_BACON}': '1',
             '${spam_eggs_sausage}': '2',
             '${spam_eggs_SAUSAGE}': '2',
             '${spam_EGGS_sausage}': '2',
             '${spam_EGGS_SAUSAGE}': '2',
             '${SPAM_eggs_sausage}': '2',
             '${SPAM_eggs_SAUSAGE}': '2',
             '${SPAM_EGGS_sausage}': '2',
             '${SPAM_EGGS_SAUSAGE}': '2',
             '${spam_brandy}': '3',
             '${spam_BRANDY}': '3',
             '${SPAM_brandy}': '3',
             '${SPAM_BRANDY}': '3'}
        )

    def test_namedtuple(self):
        self.assertEqual(attrs_to_env(
            namedtuple('Restaurant', 'spam')(spam=namedtuple('Spam', 'eggs brandy')(
                eggs=namedtuple('Eggs', 'sausage bacon')(bacon=1, sausage=2),
                brandy=3,
            ))),
            {'$spam_eggs_bacon': '1',
             '$spam_eggs_BACON': '1',
             '$spam_EGGS_bacon': '1',
             '$spam_EGGS_BACON': '1',
             '$SPAM_eggs_bacon': '1',
             '$SPAM_eggs_BACON': '1',
             '$SPAM_EGGS_bacon': '1',
             '$SPAM_EGGS_BACON': '1',
             '$spam_eggs_sausage': '2',
             '$spam_eggs_SAUSAGE': '2',
             '$spam_EGGS_sausage': '2',
             '$spam_EGGS_SAUSAGE': '2',
             '$SPAM_eggs_sausage': '2',
             '$SPAM_eggs_SAUSAGE': '2',
             '$SPAM_EGGS_sausage': '2',
             '$SPAM_EGGS_SAUSAGE': '2',
             '$spam_brandy': '3',
             '$spam_BRANDY': '3',
             '$SPAM_brandy': '3',
             '$SPAM_BRANDY': '3',
             '${spam_eggs_bacon}': '1',
             '${spam_eggs_BACON}': '1',
             '${spam_EGGS_bacon}': '1',
             '${spam_EGGS_BACON}': '1',
             '${SPAM_eggs_bacon}': '1',
             '${SPAM_eggs_BACON}': '1',
             '${SPAM_EGGS_bacon}': '1',
             '${SPAM_EGGS_BACON}': '1',
             '${spam_eggs_sausage}': '2',
             '${spam_eggs_SAUSAGE}': '2',
             '${spam_EGGS_sausage}': '2',
             '${spam_EGGS_SAUSAGE}': '2',
             '${SPAM_eggs_sausage}': '2',
             '${SPAM_eggs_SAUSAGE}': '2',
             '${SPAM_EGGS_sausage}': '2',
             '${SPAM_EGGS_SAUSAGE}': '2',
             '${spam_brandy}': '3',
             '${spam_BRANDY}': '3',
             '${SPAM_brandy}': '3',
             '${SPAM_BRANDY}': '3'}
        )

    def test_inner_namedtuple(self):
        o = SimpleNamespace(spam=namedtuple('Eggs', 'eggs')(1))
        self.assertEqual(attrs_to_env(o),
                         {'$spam_eggs': '1',
                          '$spam_EGGS': '1',
                          '$SPAM_eggs': '1',
                          '$SPAM_EGGS': '1',
                          '${spam_eggs}': '1',
                          '${spam_EGGS}': '1',
                          '${SPAM_eggs}': '1',
                          '${SPAM_EGGS}': '1'})

    def test_string_o_number_iterables(self):
        o = SimpleNamespace(list=['a', 'b', 'c'],
                            tuple=('a', 1, 'c'),
                            set={'c', 'b', 'a'},  # set should become sorted list
                            iterator=iter(range(3)),
                            generator=(x for x in range(3)))
        self.assertEqual(
            attrs_to_env(o),
            {'$list': 'a,b,c',
             '$LIST': 'a,b,c',
             '$tuple': 'a,1,c',
             '$TUPLE': 'a,1,c',
             '$set': 'a,b,c',
             '$SET': 'a,b,c',
             '$iterator': '0,1,2',
             '$ITERATOR': '0,1,2',
             '$generator': '0,1,2',
             '$GENERATOR': '0,1,2',
             '${list}': 'a,b,c',
             '${LIST}': 'a,b,c',
             '${tuple}': 'a,1,c',
             '${TUPLE}': 'a,1,c',
             '${set}': 'a,b,c',
             '${SET}': 'a,b,c',
             '${iterator}': '0,1,2',
             '${ITERATOR}': '0,1,2',
             '${generator}': '0,1,2',
             '${GENERATOR}': '0,1,2'}
        )

    def test_heterogeneous_iterables(self):
        o = SimpleNamespace(list=[
            ['spam', 'eggs', namedtuple('Spam', 'eggs')('spam')],
            {'sausage': 'bacon', 'spam': None},
            None,
            '',
            'brandy'
        ])
        self.assertEqual(attrs_to_env(o),
                         {'$list_0_0': 'spam',
                          '$list_0_1': 'eggs',
                          '$list_0_2_eggs': 'spam',
                          '$list_0_2_EGGS': 'spam',
                          '$list_1_sausage': 'bacon',
                          '$list_1_SAUSAGE': 'bacon',
                          '$list_2': 'brandy',
                          '$LIST_0_0': 'spam',
                          '$LIST_0_1': 'eggs',
                          '$LIST_0_2_eggs': 'spam',
                          '$LIST_0_2_EGGS': 'spam',
                          '$LIST_1_sausage': 'bacon',
                          '$LIST_1_SAUSAGE': 'bacon',
                          '$LIST_2': 'brandy',
                          '${list_0_0}': 'spam',
                          '${list_0_1}': 'eggs',
                          '${list_0_2_eggs}': 'spam',
                          '${list_0_2_EGGS}': 'spam',
                          '${list_1_sausage}': 'bacon',
                          '${list_1_SAUSAGE}': 'bacon',
                          '${list_2}': 'brandy',
                          '${LIST_0_0}': 'spam',
                          '${LIST_0_1}': 'eggs',
                          '${LIST_0_2_eggs}': 'spam',
                          '${LIST_0_2_EGGS}': 'spam',
                          '${LIST_1_sausage}': 'bacon',
                          '${LIST_1_SAUSAGE}': 'bacon',
                          '${LIST_2}': 'brandy'})
