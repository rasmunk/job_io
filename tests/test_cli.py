import os
import subprocess
import unittest


class TestCLI(unittest.TestCase):
    def setUp(self):
        self.package_name = "jobio"
        # Install the cli
        # args = ["python3", "setup.py", "install", "--user"]
        # result = subprocess.run(args)
        # self.assertIsNotNone(result)
        # self.assertTrue(hasattr(result, "returncode"))
        # self.assertEqual(result.returncode, 0)

    def test_cli_help(self):
        args = [self.package_name, "-h"]
        result = subprocess.run(args)
        self.assertIsNotNone(result)
        self.assertTrue(hasattr(result, "returncode"))
        self.assertEqual(result.returncode, 0)

    def test_cli_run(self):
        args = [self.package_name, "run", "/bin/echo Hello World"]
        result = subprocess.run(args)
        self.assertIsNotNone(result)
        self.assertTrue(hasattr(result, "returncode"))
        self.assertEqual(result.returncode, 0)

        # Multiple commands
        args.append("/bin/echo Another World")
        result = subprocess.run(args)
        self.assertIsNotNone(result)
        self.assertTrue(hasattr(result, "returncode"))
        self.assertEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
