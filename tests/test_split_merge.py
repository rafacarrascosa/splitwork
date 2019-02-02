import unittest

import _split_merge

from .common import _read, _write, BaseTestSplitwork


class TestSplitLines(BaseTestSplitwork, unittest.TestCase):
    def send_retrieve(self, content, n):
        inname, *outnames = self.tempfiles(n + 1)
        _write(inname, content)
        fd, = self.get_fds(inname, "r")
        outs = self.get_fds(outnames, "w")
        retcode = _split_merge.split_lines(fd, outs)
        self.assertEqual(retcode, None)
        outputs = [_read(x) for x in outnames]
        return outputs

    def test_simple_round_robin(self):
        content = "1\n2\n3\n4\n5\n6\n"
        a, b, c = self.send_retrieve(content, 3)
        self.assertEqual("1\n4\n", a)
        self.assertEqual("2\n5\n", b)
        self.assertEqual("3\n6\n", c)

    def test_single_line_multi_output(self):
        content = "12345\n"
        a, b, c = self.send_retrieve(content, 3)
        self.assertEqual("12345\n", a)
        self.assertEqual("", b)
        self.assertEqual("", c)

    def test_single_line_without_eol(self):
        content = "12345"
        a, b, c = self.send_retrieve(content, 3)
        self.assertEqual("12345", a)
        self.assertEqual("", b)
        self.assertEqual("", c)

    def test_empty_lines(self):
        content = "\n\n12345"
        a, b, c = self.send_retrieve(content, 3)
        self.assertEqual("\n", a)
        self.assertEqual("\n", b)
        self.assertEqual("12345", c)

    def test_multibuffer_line(self):
        content = "a" * 5000 + "\n"
        a, b = self.send_retrieve(content, 2)
        self.assertEqual(content, a)
        self.assertEqual("", b)

    def test_single_output(self):
        content = "a\nb\n12345"
        a, = self.send_retrieve(content, 1)
        self.assertEqual(content, a)

    def test_empty_content(self):
        content = ""
        a, b = self.send_retrieve(content, 2)
        self.assertEqual(content, a)
        self.assertEqual(content, b)

    def test_no_output(self):
        inname, = self.tempfiles(1)
        fd, = self.get_fds(inname, "r")
        with self.assertRaises(ValueError):
            _split_merge.split_lines(fd, [])


class TestMergeLines(BaseTestSplitwork, unittest.TestCase):
    def send_retrieve(self, contents):
        *innames, outname = self.tempfiles(len(contents) + 1)
        for content, name in zip(contents, innames):
            _write(name, content)
        fd, = self.get_fds(outname, "w")
        outs = self.get_fds(innames, "r")
        retcode = _split_merge.merge_lines(fd, outs)
        self.assertEqual(retcode, None)
        return _read(outname)

    def test_simple_round_robin(self):
        contents = [
            "1\nA\n",
            "2\nB\n",
            "3\nC\n",
        ]
        merged = self.send_retrieve(contents)
        self.assertEqual("1\n2\n3\nA\nB\nC\n", merged)

    def test_adds_newline_if_needed(self):
        contents = [
            "aaa",  # no newline
            "bbb",  # no newline
            "ccc",
        ]
        merged = self.send_retrieve(contents)
        self.assertEqual("aaa\nbbb\nccc", merged)

    def test_empty_lines(self):
        contents = [
            "\n2\n\n",  # no newline
            "\n\n3\n",  # no newline
            "1\n\n\n",
        ]
        merged = self.send_retrieve(contents)
        self.assertEqual("\n\n1\n2\n\n\n\n3\n\n", merged)

    def test_no_input(self):
        outname = self.tempfiles(1)
        fd, = self.get_fds(outname, "w")
        with self.assertRaises(ValueError):
            _split_merge.merge_lines(fd, [])

    def test_empty_input(self):
        contents = [
            "",
            "",
            "",
        ]
        merged = self.send_retrieve(contents)
        self.assertEqual("", merged)

    def test_different_lenghts(self):
        contents = [
            "1\n",
            "2\n2\n",
            "3\n3\n3\n",
        ]
        merged = self.send_retrieve(contents)
        self.assertEqual("1\n2\n3\n2\n3\n3\n", merged)

    def test_multibuffer_line(self):
        contents = [
            "a" * 5000 + "\n",
            "b" * 5000 + "\n",
        ]
        merged = self.send_retrieve(contents)
        self.assertEqual("".join(contents), merged)

    def test_single_input(self):
        contents = ["a\nb\n12345"]
        merged = self.send_retrieve(contents)
        self.assertEqual(contents[0], merged)


if __name__ == '__main__':
    unittest.main()
