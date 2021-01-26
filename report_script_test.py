import unittest
from report_script import run
import os
import shutil


class TestSum(unittest.TestCase):

    def test_1(self):
        # when
        run('test/test1_new', 'test/test1_old', ',', [1], 4, '/test/output')

        # then
        with open('test/output/absent.txt') as absent:
            absent_lines = absent.readlines()
            self.assertEqual(4, len(absent_lines))
            self.assertEqual('i\n', absent_lines[0])
            self.assertEqual('f\n', absent_lines[1])
            self.assertEqual('d\n', absent_lines[2])
            self.assertEqual('b\n', absent_lines[3])

        with open('test/output/differences.txt') as diff:
            diff_line = diff.readlines()
            self.assertEqual(1, len(diff_line))
            self.assertEqual('a,34 != 10\n', diff_line[0])

        shutil.rmtree('test/output')


if __name__ == '__main__':
    unittest.main()