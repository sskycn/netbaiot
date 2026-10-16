import tempfile
import unittest
from pathlib import Path

from macos_cpu_summary import summarize


class TimeProfileSummaryTest(unittest.TestCase):
    def test_reference_weights_exclude_waits_and_missing_stacks(self):
        xml = '''<table>
          <row><thread-state id="running">Running</thread-state>
            <weight id="one">1000000</weight><tagged-backtrace id="stack">
              <frame id="leaf" name="sendto"/><frame name="caller"/>
            </tagged-backtrace></row>
          <row><thread-state ref="running"/><weight ref="one"/>
            <tagged-backtrace ref="stack"/></row>
          <row><thread-state ref="running"/><weight ref="one"/></row>
          <row><thread-state>Waiting</thread-state><weight>9000000</weight>
            <tagged-backtrace><frame ref="leaf"/></tagged-backtrace></row>
        </table>'''
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'profile.xml'
            path.write_text(xml)
            result = summarize(path)
        self.assertEqual(result['active_sampled_ms'], 2)
        self.assertEqual(result['missing_stack_ms'], 1)
        self.assertEqual(result['leaf'], [dict(symbol='sendto', ms=2, percent=100)])
        self.assertEqual({x['symbol']: x['ms'] for x in result['inclusive']},
                         {'sendto': 2, 'caller': 2})
        self.assertEqual(result['states_ns'], {'Running': 3000000, 'Waiting': 9000000})

    def test_recursive_frame_is_not_counted_twice_in_inclusive_weight(self):
        xml = '''<table><row><thread-state>Running</thread-state><weight>1000000</weight>
          <tagged-backtrace><frame name="recursive"/><frame name="recursive"/>
          </tagged-backtrace></row></table>'''
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'profile.xml'
            path.write_text(xml)
            result = summarize(path)
        self.assertEqual(result['inclusive'],
                         [dict(symbol='recursive', ms=1, percent=100)])


if __name__ == '__main__':
    unittest.main()
