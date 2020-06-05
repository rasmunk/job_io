import unittest
from jobio.job import process, submit


class TestJOB(unittest.TestCase):

    def test_process(self):
        execute_kwargs = dict(command="/bin/echo", args=["Hello World"], capture=True)
        results = process(execute_kwargs=execute_kwargs)
        print(results)
        expected_results = dict(
            command="/bin/echo Hello World",
            returncode="0",
            error="b''",
            output="b'Hello World\\n'",
        )

        self.assertDictEqual(results, expected_results)
