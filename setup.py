import os
import sys
import glob
import platform

if len(sys.argv) > 1 and sys.argv[1] == 'test':
    import setuptools

from distutils.core import setup, Extension

sources = ["tarantool_snapshot.c"]
include_dirs = []
library_dirs = []
extra_compile_args = ["-D__STDC_FORMAT_MACROS", "-D__STDC_LIMIT_MACROS"]
extra_link_args = ["-static-libgcc"]

include_dirs += [
    os.path.join("msgpuck"),
    os.path.join("zstd/lib"),
    os.path.join("zstd/lib/common"),
]
sources += [
    os.path.join("msgpuck", "msgpuck.c"),
    os.path.join("msgpuck", "hints.c"),
]
sources += glob.glob("zstd/lib/common/*.c")
sources += glob.glob("zstd/lib/decompress/*.c")
extra_compile_args += ["-std=c99"]

module1 = Extension('tarantool17_snapshot',
                    include_dirs = include_dirs,
                    library_dirs = library_dirs,
                    sources = sources,
                    extra_link_args = extra_link_args,
                    extra_compile_args = extra_compile_args)

if platform.python_implementation() == "PyPy":
    interpreter = "pypy"
else:
    interpreter = "python"

if sys.version_info.major == 3:
    interpreter += "3"

setup(name = '%s-tarantool17-snapshot' % interpreter,
    description = 'Tarantool 1.6+ snapshot reader',
    version='1.5',
    author='Victor Luchits',
    author_email='vluchits@gmail.com',
    url='https://github.com/viciious/python-tarantool16_snaphot',
    packages=[],
    ext_modules = [module1])

