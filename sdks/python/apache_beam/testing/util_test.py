#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Unit tests for testing utilities."""

import unittest

from apache_beam import Create
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import TestWindowedValue
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import is_empty
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import IntervalWindow
from apache_beam.utils.timestamp import MIN_TIMESTAMP

# debugging imports
from apache_beam.transforms import window
from apache_beam.testing.util import equal_to_per_window
from apache_beam.utils.windowed_value import WindowedValue
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.core import WindowInto
from apache_beam.transforms.core import Map
from apache_beam.transforms.core import ParDo
from apache_beam.transforms.core import DoFn


class UtilTest(unittest.TestCase):

  def test_assert_that_passes(self):
    with TestPipeline() as p:
      assert_that(p | Create([1, 2, 3]), equal_to([1, 2, 3]))

  def test_assert_that_passes_empty_equal_to(self):
    with TestPipeline() as p:
      assert_that(p | Create([]), equal_to([]))

  def test_assert_that_passes_empty_is_empty(self):
    with TestPipeline() as p:
      assert_that(p | Create([]), is_empty())

  def test_assert_that_fails(self):
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(p | Create([1, 10, 100]), equal_to([1, 2, 3]))

  # original test that I didn't get to work
  def test_windowed_value_passes(self):
    expected = {IntervalWindow(0, 1): [1, 2, 3], IntervalWindow(4, 5):['a', 'b']}
    with TestPipeline() as p:
      p = (p | 'side TestStream' >> TestStream()
            .add_elements([window.TimestampedValue('s1', 10)])
            )
      assert_that(p, equal_to_per_window(expected),
                custom_windowing=window.FixedWindows(30), reify_windows=False)

  def test_windowed_value_passes2(self):
    # expected = [(v, FixedWindow(1))
    expected = {IntervalWindow(0, 1): [1, 2, 3], IntervalWindow(4, 5):['a', 'b']}
    print 'expected:', expected

    class PrintDo(DoFn):
      def process(self, element, timestamp=DoFn.TimestampParam,
                  window=DoFn.WindowParam):
        print 'element, window, timestamp:', element, window, timestamp
        yield element


    with TestPipeline() as p:

      # p = (p | 'side TestStream' >> TestStream()
      #       .add_elements([window.TimestampedValue('s1', 10)])
      #       )
      p = p | Create(['s1'])
      p = p | Map(lambda e: window.TimestampedValue(e, 10))
      p = p | ParDo(PrintDo())

      p = p | WindowInto(window.FixedWindows(30))
      p = p | 'AFTER' >> ParDo(PrintDo())
      p = p | 'AFTER2' >> ParDo(PrintDo())

      assert_that(p, equal_to_per_window(expected),
                custom_windowing=window.FixedWindows(30), reify_windows=False)
      # p.run()

  def test_reified_value_passes(self):
    expected = [TestWindowedValue(v, MIN_TIMESTAMP, [GlobalWindow()])
                for v in [1, 2, 3]]
    with TestPipeline() as p:
      assert_that(p | Create([2, 3, 1]), equal_to(expected), reify_windows=True)

  def test_reified_value_assert_fail_unmatched_value(self):
    expected = [TestWindowedValue(v + 1, MIN_TIMESTAMP, [GlobalWindow()])
                for v in [1, 2, 3]]
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(p | Create([2, 3, 1]), equal_to(expected),
                    reify_windows=True)

  def test_reified_value_assert_fail_unmatched_timestamp(self):
    expected = [TestWindowedValue(v, 1, [GlobalWindow()])
                for v in [1, 2, 3]]
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(p | Create([2, 3, 1]), equal_to(expected),
                    reify_windows=True)

  def test_reified_value_assert_fail_unmatched_window(self):
    expected = [TestWindowedValue(v, MIN_TIMESTAMP, [IntervalWindow(0, 1)])
                for v in [1, 2, 3]]
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(p | Create([2, 3, 1]), equal_to(expected),
                    reify_windows=True)

  def test_assert_that_fails_on_empty_input(self):
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(p | Create([]), equal_to([1, 2, 3]))

  def test_assert_that_fails_on_empty_expected(self):
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(p | Create([1, 2, 3]), is_empty())


if __name__ == '__main__':
  unittest.main()
