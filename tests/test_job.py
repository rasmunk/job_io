import unittest
from jobio.job import process


class TestJOB(unittest.TestCase):
    def test_process(self):
        execute_kwargs = dict(commands=["/bin/echo Hello World"], capture=True)
        results = process(execute_kwargs=execute_kwargs)
        expected_results = dict(
            command="/bin/echo Hello World",
            returncode="0",
            error="b''",
            output="b'Hello World\\n'",
        )

        self.assertDictEqual(results[0], expected_results)

    def test_process_multiple_commands(self):
        execute_kwargs = dict(
            commands=["/bin/echo Hello World", "/bin/echo Another World"], capture=True
        )
        results = process(execute_kwargs=execute_kwargs)
        first_results = dict(
            command="/bin/echo Hello World",
            returncode="0",
            error="b''",
            output="b'Hello World\\n'",
        )
        self.assertDictEqual(results[0], first_results)

        second_results = dict(
            command="/bin/echo Another World",
            returncode="0",
            error="b''",
            output="b'Another World\\n'",
        )
        self.assertDictEqual(results[1], second_results)
