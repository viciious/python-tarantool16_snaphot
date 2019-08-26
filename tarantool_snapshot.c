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
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <fcntl.h>

#include <msgpuck.h>
#include <zstd.h>

#define FADVD_WINDOW_SIZE ( 10 * 1024 * 1024 )

/*
 * marker is MsgPack fixext2
 * +--------+--------+--------+--------+
 * |  0xd5  |  type  |       data      |
 * +--------+--------+--------+--------+
 */
typedef uint32_t log_magic_t;
enum { HEADER_LEN_MAX = 40, BODY_LEN_MAX = 128 };


enum {
    /** Maximal iproto package body length (2GiB) */
    IPROTO_BODY_LEN_MAX = 2147483648UL,
    /* Maximal length of text handshake (greeting) */
    IPROTO_GREETING_SIZE = 128,
    /** marker + len + prev crc32 + cur crc32 + (padding) */
    XLOG_FIXHEADER_SIZE = 19
};

static const log_magic_t row_marker  = mp_bswap_u32(0xd5ba0bab);
static const log_magic_t zrow_marker = mp_bswap_u32(0xd5ba0bba);
static const log_magic_t eof_marker  = mp_bswap_u32(0xd510aded);

static PyObject *SnapshotError;

typedef struct {
    ZSTD_DStream* d_stream;
    /* use ZSTD_outBuffer as input buffer cause it has non-const void* */
    ZSTD_outBuffer inbuf;
    ZSTD_outBuffer outbuf;
    size_t inbuf_realsize;
    size_t error;
} SnapshotZstd;

typedef struct {
    PyObject_HEAD
    FILE *f;
    int fileh;
    off_t prevfadv;
    char *filename;
    int open_exception;
    int version;
    SnapshotZstd zstd;
    char *msgp_pos;
    char *msgp_end;
    char error_buf[256];
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
    "tarantool17_snapshot.SnapshotIterator",      /*tp_name*/
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
    static const char v13[] = "0.13\n";
    SnapshotZstd *zstd = &self->zstd;

    self->f = NULL;
    self->open_exception = 0;
    self->prevfadv = 0;
    self->fileh = -1;
    self->version = 0;
    self->error_buf[0] = 0;
    memset(zstd, 0, sizeof(*zstd));
    self->msgp_pos = self->msgp_end = NULL;

    if (!PyArg_ParseTuple(args, "s", &self->filename)) {
        return 1;
    }

    if ((f = fopen(self->filename, "rb")) == NULL) {
        snprintf(self->error_buf, sizeof(self->error_buf), "can't open for reading");
        goto error;
    }

    if (fgets(filetype, sizeof(filetype), f) == NULL ||
        fgets(version, sizeof(version), f) == NULL) {
        snprintf(self->error_buf, sizeof(self->error_buf), "error reading file header");
        goto error;
    }

    if (strncmp(filetype, "SNAP", 4) && strncmp(filetype, "XLOG", 4)) {
        snprintf(self->error_buf, sizeof(self->error_buf), "unknown file header: expected SNAP or XLOG");
        goto error;
    }

    if (strcmp(v12, version) == 0) {
        self->version = 12;
    } else if (strcmp(v13, version) == 0) {
        self->version = 13;
    } else {
        snprintf(self->error_buf, sizeof(self->error_buf), "unknown header version: %s", version);
        goto error;
    }

    if (self->version == 12) {
        zstd->inbuf_realsize = 0;
        zstd->inbuf.size = zstd->inbuf.pos = zstd->inbuf_realsize;
    } else if (self->version == 13) {
        zstd->d_stream = ZSTD_createDStream();
        if (!zstd->d_stream) {
            snprintf(self->error_buf, sizeof(self->error_buf), "can't create zstd stream");
            goto error;
        }

        zstd->error = ZSTD_initDStream(zstd->d_stream);
        if (ZSTD_isError(zstd->error)) {
            snprintf(self->error_buf, sizeof(self->error_buf), "zstd error: %s", ZSTD_getErrorName(zstd->error));
            goto error;
        }

        zstd->inbuf_realsize = ZSTD_DStreamInSize();
        zstd->inbuf.size = zstd->inbuf.pos = zstd->inbuf_realsize;

        zstd->outbuf.size = ZSTD_DStreamOutSize();
        zstd->outbuf.pos = 0;

        zstd->inbuf.dst = (uint8_t*)malloc(zstd->inbuf_realsize);
        zstd->outbuf.dst = (uint8_t*)malloc(zstd->outbuf.size);
        if (!zstd->inbuf.dst || !zstd->outbuf.dst) {
            snprintf(self->error_buf, sizeof(self->error_buf), "out of memory");
            goto error;
        }
    }

    for (;;) {
        if (fgets(buf, sizeof(buf), f) == NULL) {
            snprintf(self->error_buf, sizeof(self->error_buf), "can't read header line");
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

    if (f != NULL) {
        self->f = f;
        self->fileh = fileno(f);
    }
    return self->open_exception;
}

static int header_decode(SnapshotIterator *self, PyObject **dest, const char **pos, const char *end)
{
    const char *meta_pos = *pos;

    if (mp_typeof(**pos) != MP_MAP) {
error:
        snprintf(self->error_buf, sizeof(self->error_buf), "expected msgpack map, got something else");
        return -1;
    }

    mp_next(pos);

    if (mp_typeof(**pos) != MP_MAP) {
        goto error;
    }

    const char *meta_end = *pos;
    const char *row_pos = *pos;

    mp_next(pos);

    assert(*pos <= end);
    if (*pos > end) {
        snprintf(self->error_buf, sizeof(self->error_buf), "msgpack buffer overrun");
        return 1;
    }

    const char *row_end = *pos;

    *dest = Py_BuildValue("(s#,s#)", meta_pos,  meta_end - meta_pos, row_pos, row_end - row_pos);

    return 0;
}

/**
 * @retval 0 success
 * @retval 1 EOF
 */
static int SnapshotIterator_readrow(SnapshotIterator *self, PyObject **dest, char compressed)
{
    const char *data;
    FILE *f = self->f;
    SnapshotZstd *zstd = &self->zstd;

    /* Read fixed header */
    char fixheader[XLOG_FIXHEADER_SIZE - sizeof(log_magic_t)];
    if (fread(fixheader, sizeof(fixheader), 1, f) != 1) {
        if (feof(f)) {
            snprintf(self->error_buf, sizeof(self->error_buf), "truncated stream");
            return 1;
        }
error:
        snprintf(self->error_buf, sizeof(self->error_buf), "failed to read or parse row header"
             " at offset %" PRIu64, (uint64_t) ftello(f));
        return -1;
    }

    /* Decode len, previous crc32 and row crc32 */
    data = fixheader;

    /* Read length */
    if (mp_typeof(*data) != MP_UINT)
        goto error;
    uint32_t len = mp_decode_uint(&data);
    if (len > IPROTO_BODY_LEN_MAX) {
        snprintf(self->error_buf, sizeof(self->error_buf),
             "row is too big at offset %" PRIu64,
             (uint64_t) ftello(f));
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

    if (len > zstd->inbuf_realsize) {
        free(zstd->inbuf.dst);
        zstd->inbuf_realsize = 0;

        zstd->inbuf.dst = malloc(len);
        if (!zstd->inbuf.dst) {
            snprintf(self->error_buf, sizeof(self->error_buf), "out of memory");
            return 1;
        }
        zstd->inbuf_realsize = len;
    }

    if (compressed) {
        if (len < 4) {
            snprintf(self->error_buf, sizeof(self->error_buf), "truncated compressed row header");
            return 1;
        }

        zstd->inbuf.pos = 0;
        zstd->inbuf.size = len;
    } else {
        zstd->inbuf.pos = len;
        zstd->inbuf.size = len;

        zstd->outbuf.pos = len;
        zstd->outbuf.size = len;
        zstd->outbuf.dst = zstd->inbuf.dst;
    }

    if (fread(zstd->inbuf.dst, len, 1, f) != 1) {
        snprintf(self->error_buf, sizeof(self->error_buf), "truncated row");
        return 1;
    }

#if ( _XOPEN_SOURCE >= 600 || _POSIX_C_SOURCE >= 200112L )
    off_t curpos = ftell(f);
    if (curpos >= self->prevfadv + FADVD_WINDOW_SIZE) {
        posix_fadvise(self->fileh, self->prevfadv, curpos, POSIX_FADV_DONTNEED);
        self->prevfadv = curpos;
    }
#endif

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
    int res = -1;
    FILE *f = self->f;
    log_magic_t magic;
    char compressed = 0;
    SnapshotZstd *zstd = &self->zstd;

    if (self->msgp_pos < self->msgp_end) {
        goto decode_row;
    }
    if (zstd->inbuf.pos < zstd->inbuf.size) {
        goto decompress_row;
    }

    if (fread(&magic, sizeof(magic), 1, f) != 1) {
        snprintf(self->error_buf, sizeof(self->error_buf), "truncated stream");
        return -1;
    }

    while (1) {
        if (magic == eof_marker) {
            return 1;
        }
        if (magic == row_marker) {
            break;
        }
        if (self->version == 13 && magic == zrow_marker) {
            compressed = 1;
            break;
        }

        int c = fgetc(f);
        if (c == EOF) {
            snprintf(self->error_buf, sizeof(self->error_buf), "truncated stream");
            return -1;
        }
        magic = magic >> 8 |
            ((log_magic_t) c & 0xff) << (sizeof(magic)*8 - 8);
    }

    res = SnapshotIterator_readrow(self, dest, compressed);
    if (res < 0) {
        return res;
    }
    if (res != 0) {
        return -res;
    }

decompress_row:
    if (zstd->inbuf.pos < zstd->inbuf.size) {
        zstd->outbuf.pos = 0;
        zstd->error = ZSTD_decompressStream(zstd->d_stream, &zstd->outbuf, (ZSTD_inBuffer*)&zstd->inbuf);
        if (ZSTD_isError(zstd->error)) {
            snprintf(self->error_buf, sizeof(self->error_buf), "zstd error: %s", ZSTD_getErrorName(zstd->error));
            return -1;
        }
    }

    self->msgp_pos = (char*)zstd->outbuf.dst;
    self->msgp_end = self->msgp_pos + zstd->outbuf.pos;

decode_row:
    assert(self->msgp_pos < self->msgp_end);

    if (self->msgp_pos < self->msgp_end) {
       const char *data = self->msgp_pos;
       if (header_decode(self, dest, &data, self->msgp_end) != 0)
            return -1;
       self->msgp_pos = (char *)data;
       return 0;
    }

    return -1;
}

PyObject* SnapshotIterator_iter(SnapshotIterator* self) {
    if (self->open_exception) {
        PyErr_Format(SnapshotError, "Error opening '%s': %s", self->filename, self->error_buf);
        return NULL;
    }
    Py_INCREF(self);
    return (PyObject*)self;
}

PyObject* SnapshotIterator_iternext(SnapshotIterator* self) {
    PyObject *s = NULL;

    int res = SnapshotIterator_nextrow(self, &s);
    if (res == 0) {
        return s;
    }
    if (res == 1) {
        // EOF
        return NULL;
    }

    PyErr_Format(SnapshotError, "Error reading '%s': %s", self->filename, self->error_buf);
    return NULL;
}

PyObject* SnapshotIterator_del(SnapshotIterator* self) {
    SnapshotZstd *zstd = &self->zstd;

    if (self->f) {
#if ( _XOPEN_SOURCE >= 600 || _POSIX_C_SOURCE >= 200112L )
        posix_fadvise(self->fileh, (off_t) 0, (off_t) 0, POSIX_FADV_DONTNEED);
#endif
        fclose(self->f);
    }

    if (self->version == 13) {
        if (zstd->d_stream) {
            ZSTD_freeDStream(zstd->d_stream);
        }
        if (zstd->outbuf.dst && zstd->outbuf.dst != zstd->inbuf.dst) {
            free(zstd->outbuf.dst);
        }
    }

    if (zstd->inbuf.dst) {
        free(zstd->inbuf.dst);
    }

    PyObject_Del(self);
    Py_RETURN_NONE;
}

static PyMethodDef TarantoolSnapshot_Module_Methods[] = {
    {NULL}  /* Sentinel */
};

PyMODINIT_FUNC inittarantool17_snapshot(void) {
    PyObject *m;

    SnapshotIterator_Type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&SnapshotIterator_Type) < 0)  return;

    m = Py_InitModule("tarantool17_snapshot", TarantoolSnapshot_Module_Methods);
    if (!m) {
        return;
    }

    Py_INCREF(&SnapshotIterator_Type);
    PyModule_AddObject(m, "iter", (PyObject *)&SnapshotIterator_Type);

    SnapshotError = PyErr_NewException((char *)"tarantool17_snapshot.SnapshotError", (PyObject *)NULL, (PyObject *)NULL);
    Py_INCREF(SnapshotError);
    PyModule_AddObject(m, "SnapshotError", SnapshotError); 
}

int main(int argc, char **argv) {
    Py_SetProgramName(argv[0]);
    Py_Initialize();
    inittarantool17_snapshot();
    return 0;
}
