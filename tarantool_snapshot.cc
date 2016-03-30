/*
 * Copyright 2010-2015, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include <Python.h>

#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

#include <crc32.h>
#include <iproto_constants.h>

/*
 * marker is MsgPack fixext2
 * +--------+--------+--------+--------+
 * |  0xd5  |  type  |       data      |
 * +--------+--------+--------+--------+
 */
typedef uint32_t log_magic_t;
enum { HEADER_LEN_MAX = 40, BODY_LEN_MAX = 128 };

static PyObject *SnapshotError;

typedef struct {
    PyObject_HEAD
    FILE *f;
    char *filename;
    int open_exception;
} SnapshotIterator;

static PyMethodDef SnapshotIterator_Methods[] = {
    {NULL}  /* Sentinel */
};

static int SnapshotIterator_init(SnapshotIterator *self, PyObject *args);
PyObject* SnapshotIterator_del(SnapshotIterator *self);
PyObject* SnapshotIterator_iter(SnapshotIterator *self);
PyObject* SnapshotIterator_iternext(SnapshotIterator *self);

static PyTypeObject SnapshotIterator_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "tarantool16_snapshot.SnapshotIterator",      /*tp_name*/
    sizeof(SnapshotIterator),      /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)SnapshotIterator_del,             /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_ITER,
        /*
         * tp_flags: Py_TPFLAGS_HAVE_ITER tells python to
         * use tp_iter and tp_iternext fields.
        */
    "SnapshotIterator",                /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    (getiterfunc)SnapshotIterator_iter,  /* tp_iter: __iter__() method */
    (iternextfunc)SnapshotIterator_iternext,  /* tp_iternext: next() method */
    SnapshotIterator_Methods,          /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)SnapshotIterator_init,   /* tp_init */
    0                          /* tp_alloc */
};

static int SnapshotIterator_init(SnapshotIterator *self, PyObject *args) {
    char filetype[32], version[32];
    char buf[256];
    FILE *f = NULL;
    static const char v12[] = "0.12\n";

    self->f = NULL;
    self->open_exception = 0;

    if (!PyArg_ParseTuple(args, "s", &self->filename)) {
        return 1;
    }

    if ((f = fopen(self->filename, "rb")) == NULL) {
        goto error;
    }
    if (fgets(filetype, sizeof(filetype), f) == NULL ||
        fgets(version, sizeof(version), f) == NULL) {
        goto error;
    }
    if (strcmp(v12, version) != 0) {
        goto error;
    }

    for (;;) {
        if (fgets(buf, sizeof(buf), f) == NULL) {
            goto error;
        }
        /** Empty line indicates the end of file header. */
        if (strcmp(buf, "\n") == 0)
            break;
        /* Skip header */
    }

    goto done;

error:
    self->open_exception = 1;
done:
    if (self->open_exception) {
        if (f) {
            fclose(f);
            f = NULL;
        }
    }

    self->f = f;
    return self->open_exception;
}

static int header_decode(PyObject **dest, const char **pos, const char *end)
{
    const char *pos2 = *pos;
    if (mp_check(&pos2, end) != 0) {
error:
        return -1;
    }

    if (mp_typeof(**pos) != MP_MAP)
        goto error;

    uint32_t size = mp_decode_map(pos);
    for (uint32_t i = 0; i < size; i++) {
        if (mp_typeof(**pos) != MP_UINT)
            goto error;
        unsigned char key = mp_decode_uint(pos);
        if (iproto_key_type[key] != mp_typeof(**pos))
            goto error;
        switch (key) {
        case IPROTO_REQUEST_TYPE:
            mp_decode_uint(pos);
            break;
        case IPROTO_SYNC:
            mp_decode_uint(pos);
            break;
        case IPROTO_SERVER_ID:
            mp_decode_uint(pos);
            break;
        case IPROTO_LSN:
            mp_decode_uint(pos);
            break;
        case IPROTO_TIMESTAMP:
            mp_decode_double(pos);
            break;
        case IPROTO_SCHEMA_ID:
            mp_decode_uint(pos);
            break;
        default:
            /* unknown header */
            mp_next(pos);
        }
    }
    assert(*pos <= end);
    if (*pos < end) {
        *dest = PyString_FromStringAndSize(*pos, end - *pos);
        *pos = end;
        return 0;
    }
    return 1;
}

/**
 * @retval 0 success
 * @retval 1 EOF
 */
static int SnapshotIterator_readrow(SnapshotIterator *self, PyObject **dest)
{
    const char *data;
    FILE *f = self->f;

    /* Read fixed header */
    char fixheader[XLOG_FIXHEADER_SIZE - sizeof(log_magic_t)];
    if (fread(fixheader, sizeof(fixheader), 1, f) != 1) {
        if (feof(f))
            return 1;
error:
        char buf[PATH_MAX];
        snprintf(buf, sizeof(buf), "%s: failed to read or parse row header"
             " at offset %" PRIu64, self->filename,
             (uint64_t) ftello(f));
        return -1;
    }

    /* Decode len, previous crc32 and row crc32 */
    data = fixheader;
    if (mp_check(&data, data + sizeof(fixheader)) != 0)
        goto error;
    data = fixheader;

    /* Read length */
    if (mp_typeof(*data) != MP_UINT)
        goto error;
    uint32_t len = mp_decode_uint(&data);
    if (len > IPROTO_BODY_LEN_MAX) {
        char buf[PATH_MAX];
        snprintf(buf, sizeof(buf),
             "%s: row is too big at offset %" PRIu64,
             self->filename, (uint64_t) ftello(f));
        return -1;
    }

    /* Read previous crc32 */
    if (mp_typeof(*data) != MP_UINT)
        goto error;

    /* Read current crc32 */
    uint32_t crc32p = mp_decode_uint(&data);
    if (mp_typeof(*data) != MP_UINT)
        goto error;
    uint32_t crc32c = mp_decode_uint(&data);
    assert(data <= fixheader + sizeof(fixheader));
    (void) crc32p;
    (void) crc32c;

    /* Allocate memory for body */
    char *bodybuf = (char *)alloca(len);

    /* Read header and body */
    if (fread(bodybuf, len, 1, f) != 1)
        return 1;

    data = bodybuf;
    if (header_decode(dest, &data, bodybuf + len) != 0)
        return 1;

    return 0;
}

/**
 * Read logfile contents using designated format, panic if
 * the log is corrupted/unreadable.
 *
 * @param i iterator object, encapsulating log specifics.
 *
 * @retval 0    OK
 * @retval 1    EOF
 */
static int SnapshotIterator_nextrow(SnapshotIterator *self, PyObject **dest)
{
    int res = 1;
    FILE *f = self->f;
    log_magic_t magic;
    static const log_magic_t row_marker = mp_bswap_u32(0xd5ba0bab); /* host byte order */

    if (fread(&magic, sizeof(magic), 1, f) != 1)
        goto eof;

    while (magic != row_marker) {
        int c = fgetc(f);
        if (c == EOF) {
            goto eof;
        }
        magic = magic >> 8 |
            ((log_magic_t) c & 0xff) << (sizeof(magic)*8 - 8);
    }

    res = SnapshotIterator_readrow(self, dest);
    if (res < 0) {
        return res;
    }
    if (res != 0) {
        goto eof;
    }

    return 0;

eof:
    return 1;
}

PyObject* SnapshotIterator_iter(SnapshotIterator* self) {
    if (self->open_exception) {
        PyErr_Format(SnapshotError, "Can't open snapshot");
        return NULL;
    }
    Py_INCREF(self);
    return (PyObject*)self;
}

PyObject* SnapshotIterator_iternext(SnapshotIterator* self) {
    PyObject *s = NULL;
    while (SnapshotIterator_nextrow(self, &s) == 0) {
        return s;
    }
    return NULL;
}

PyObject* SnapshotIterator_del(SnapshotIterator* self) {
    if (self->f)
        fclose(self->f);
    PyObject_Del(self);
    Py_RETURN_NONE;
}

static PyMethodDef TarantoolSnapshot_Module_Methods[] = {
    {NULL}  /* Sentinel */
};

PyMODINIT_FUNC inittarantool16_snapshot(void) {
    PyObject *m;

    SnapshotIterator_Type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&SnapshotIterator_Type) < 0)  return;

    m = Py_InitModule("tarantool16_snapshot", TarantoolSnapshot_Module_Methods);
    if (!m) {
        return;
    }

    Py_INCREF(&SnapshotIterator_Type);
    PyModule_AddObject(m, "iter", (PyObject *)&SnapshotIterator_Type);

    SnapshotError = PyErr_NewException((char *)"tarantool16_snapshot.SnapshotError", (PyObject *)NULL, (PyObject *)NULL);
    Py_INCREF(SnapshotError);
    PyModule_AddObject(m, "SnapshotError", SnapshotError); 
}

int main(int argc, char **argv) {
    Py_SetProgramName(argv[0]);
    Py_Initialize();
    inittarantool16_snapshot();
    return 0;
}
