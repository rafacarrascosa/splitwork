#include <Python.h>
#include <stdlib.h>

#define BLOCK_SIZE 4096
#define IS_CLOSED(r) (r->cursor == NULL)
#define MARK_AS_CLOSED(r) r->cursor = NULL; r->remaining = 0


typedef struct BufferedLineReader {
  char buffer[BLOCK_SIZE];
  int fd;
  size_t remaining;  /* the amount of bytes from cursor to the end */
  char *cursor;
} LineReader;


typedef struct BufferedWriter {
  char buffer[BLOCK_SIZE];
  int fd;
  size_t remaining;  /* the amount of bytes from cursor to the end */
  char *cursor;
} Writer;


static inline void line_reader_init(LineReader *r, int fd) {
  r->fd = fd;
  r->remaining = 0;
  r->cursor = r->buffer;
}


static inline ssize_t line_reader_load_buffer(LineReader *r) {
  ssize_t count = -1;
  r->remaining = 0;
  r->cursor = r->buffer;
  while (1) {
    count = read(r->fd, &(r->buffer), BLOCK_SIZE);
    if (count > 0) {
      break;
    } else if(count == -1 && errno == EINTR) {
      if (PyErr_CheckSignals()) return -1;
    } else {
      return count;
    }
  }
  r->remaining = count;
  return count;
}


static inline size_t line_reader_next_line(LineReader *r, size_t *size) {
  char *endptr = memchr(r->cursor, '\n', r->remaining);
  if (endptr == NULL) {
    *size = r->remaining;
    return 0;
  }
  *size = endptr - r->cursor + 1;
  return 1;
}


static inline void line_reader_advance(LineReader *r, size_t offset) {
  r->remaining -= offset;
  r->cursor += offset;
}


static inline int line_reader_at_end_of_buffer(LineReader *r) {
  return (r->remaining == 0);
}


static inline void writer_init(Writer *w, int fd) {
  w->fd = fd;
  w->remaining = BLOCK_SIZE;
  w->cursor = w->buffer;
}


static inline ssize_t writer_write(Writer *w, const char *data, size_t len) {
  ssize_t result = 0, retval = 0;
  if (w->remaining <= len) {
    retval = w->remaining;  /* Only some bytes are copied and thus the return value */
    memcpy(w->cursor, data, w->remaining);
    result = write(w->fd, w->buffer, BLOCK_SIZE);
    if (result <= 0) {
      return result;
    }
    if (result < BLOCK_SIZE) {  /* Assuming this case is unfrequent */
      memmove(w->buffer, w->buffer + result, BLOCK_SIZE - result);
    }
    w->remaining = result;
    w->cursor = w->buffer + (BLOCK_SIZE - result);
  } else {
    memcpy(w->cursor, data, len);
    w->remaining -= len;
    w->cursor += len;
    retval = len;
  }
  return retval;
}


static inline ssize_t writer_flush(Writer *w) {
  char *cursor;
  size_t left_to_write = 0;
  ssize_t result = 0;
  left_to_write = BLOCK_SIZE - w->remaining;
  cursor = w->buffer;
  while (left_to_write) {
    result = write(w->fd, cursor, left_to_write);
    if (result <= 0) {  /* Assuming this case is unfrequent */
      memmove(w->buffer, cursor, left_to_write);
      w->remaining = BLOCK_SIZE - left_to_write;
      w->cursor = w->buffer + left_to_write;
      return result;
    }
    left_to_write -= result;
    cursor += result;
  }
  return BLOCK_SIZE - w->remaining;
}


static inline int _split_lines_impl(int fd, Writer *out_writers, size_t n) {
  size_t i, size;
  ssize_t written = 1;
  int has_newline;
  LineReader reader;
  line_reader_init(&reader, fd);

  i = 0;
  if(line_reader_load_buffer(&reader) < 0) return -1;
  while (reader.remaining) {
    while (reader.remaining) {
      has_newline = line_reader_next_line(&reader, &size);
      written = writer_write(out_writers + i, reader.cursor, size);
      if (written == -1 && errno == EINTR) {
        if (PyErr_CheckSignals()) return -1;
        continue;  /* retry */
      }
      if (written <= 0) break;
      line_reader_advance(&reader, written);
      if (has_newline && (written == size)) {
        i = (i + 1) % n;
      }
    }
    if (PyErr_CheckSignals()) return -1;
    if (written <= 0) break;
    if(line_reader_load_buffer(&reader) < 0) return -1;
  }
  if (written <= 0) {
    return i + 1;  /* indicates the position of the offending fd + 1 */
  }
  i = 0;
  while (i < n) {
    written = writer_flush(out_writers + i);
    if (written == -1 && errno == EINTR) {
      if (PyErr_CheckSignals()) return -1;
      continue;  /* retry */
    }
    if (written < 0) {
      return i + 1;  /* indicates the position of the offending fd + 1 */
    }
    i += 1;
  }
  return 0;
}


static inline LineReader * _get_next_ready(LineReader *readers, size_t n, size_t *i, size_t *still_open) {
  LineReader *current = readers + *i;
  while (*still_open && line_reader_at_end_of_buffer(current)) {
    if (IS_CLOSED(current)) {
      *i = (*i + 1) % n;
      current = readers + *i;
      continue;
    }
    if(line_reader_load_buffer(current) < 0) return NULL;
    if (current->remaining == 0) {
      MARK_AS_CLOSED(current);
      *still_open -= 1;
      *i = (*i + 1) % n;
      current = readers + *i;
    } else {
      break;
    }
  }
  if (*still_open == 0) {
    return NULL;
  }
  return current;
}


static inline int _merge_lines_impl(int fd, LineReader *readers, size_t n) {
  size_t i = 0, still_open, size;
  ssize_t result, written = 1;
  int has_newline;
  LineReader *current = NULL;
  Writer w;

  writer_init(&w, fd);
  still_open = n;
  while(still_open) {
    if (PyErr_CheckSignals()) return -1;
    current = _get_next_ready(readers, n, &i, &still_open);
    if (current == NULL) {
      if (still_open > 0) return -1; /* interruption propagation */
      break;
    }
    has_newline = line_reader_next_line(current, &size);
    written = write(fd, current->cursor, size);
    if (written <= 0) break;
    line_reader_advance(current, written);
    if (has_newline && (written == size)) {
      i = (i + 1) % n;
    } else if (!has_newline &&
               still_open > 1 &&
               line_reader_at_end_of_buffer(current)) {
      if(line_reader_load_buffer(current) < 0) return -1;
      /* When a fd ends without newline and there are still other files open */
      if (current->remaining == 0) {
        written = write(fd, "\n", 1);
        if (written <= 0) break;
        MARK_AS_CLOSED(current);
        still_open -= 1;
        i = (i + 1) % n;
      }
    }
  }

  if (written == 0) return 1;
  return 0;
}


static PyObject * split_lines(PyObject *self, PyObject *args)
{
  size_t i = 0, n = 0;
  int fd, result;
  PyObject *lst = NULL;
  Writer *out_writers = NULL;

  if (!PyArg_ParseTuple(args, "iO", &fd, &lst)) {
      return NULL;
  }

  n = PyObject_Length(lst);
  if (n <= 0) {
      PyErr_SetString(PyExc_ValueError, "output file list is empty");
      return NULL;
  }

  out_writers = malloc(sizeof(Writer) * n);
  if (out_writers == NULL) {
    PyErr_NoMemory();
    return NULL;
  }

  for (i = 0; i < n; i++) {
      writer_init(out_writers + i, (int) PyLong_AsLong(PyList_GetItem(lst, i)));
      if (PyErr_Occurred()) {
        free(out_writers);
        return NULL;
      }
  }

  result = _split_lines_impl(fd, out_writers, n);
  free(out_writers);
  if (result > 0) {
    PyErr_SetObject(PyExc_IOError, Py_BuildValue("si", "Write error in file descriptor", out_writers[result - 1].fd));
    return NULL;
  } else if (result < 0) {
    return NULL;
  }
  return Py_BuildValue("");
}


static PyObject * merge_lines(PyObject *self, PyObject *args)
{
  size_t i = 0, n = 0;
  int fd, result;
  LineReader *readers;
  PyObject *lst = NULL;

  if (!PyArg_ParseTuple(args, "iO", &fd, &lst)) {
      return NULL;
  }

  n = PyObject_Length(lst);
  if (n <= 0) {
      PyErr_SetString(PyExc_ValueError, "input file list is empty");
      return NULL;
  }

  readers = malloc(sizeof(LineReader) * n);
  if (readers == NULL) {
    PyErr_NoMemory();
    return NULL;
  }

  for (i = 0; i < n; i++) {
      line_reader_init(readers + i, (int) PyLong_AsLong(PyList_GetItem(lst, i)));
      if (PyErr_Occurred()) {
        free(readers);
        return NULL;
      }
  }

  result = _merge_lines_impl(fd, readers, n);
  free(readers);
  if (result > 0) {
    PyErr_SetObject(PyExc_IOError, Py_BuildValue("si", "Read error in file descriptor", readers[result - 1].fd));
    return NULL;
  } else if (result < 0) {
    return NULL;
  }
  return Py_BuildValue("");
}


static PyMethodDef Module_FunctionsTable[] = {
    {
        "split_lines", // name exposed to Python
        split_lines, // C wrapper function
        METH_VARARGS, // received variable args
        "Splits lines from fd into multiple output fds" // documentation
    },
    {
        "merge_lines", // name exposed to Python
        merge_lines, // C wrapper function
        METH_VARARGS, // received variable args
        "Merge lines from multiple fds into output fd" // documentation
    }, {
        NULL, NULL, 0, NULL
    }
};

// modules definition
static struct PyModuleDef Module = {
    PyModuleDef_HEAD_INIT,
    "_split_lines",     // name of module exposed to Python
    "Python wrapper for fast C I/O split and merge", // module documentation
    -1,
    Module_FunctionsTable
};

PyMODINIT_FUNC PyInit__split_merge(void) {
    return PyModule_Create(&Module);
}
