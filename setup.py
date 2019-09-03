import os
import sys
import glob
from setuptools.command.bdist_rpm import bdist_rpm

RPM_REQUIRED_DEPS = "python-msgpack"

## HACK FOR DEPS IN RPMS
def custom_make_spec_file(self):
    spec = self._original_make_spec_file()
    lineDescription = "%description"
    spec.insert(spec.index(lineDescription) - 1, "Requires: %s" % RPM_REQUIRED_DEPS)
    return spec
bdist_rpm._original_make_spec_file = bdist_rpm._make_spec_file
bdist_rpm._make_spec_file = custom_make_spec_file
## END OF HACK

try:
    from setuptools import setup, Extension
    extra_params = dict(test_suite = 'tests',)
except ImportError:
    from distutils.core import setup, Extension
    extra_params = {}

def sh(command):
    import subprocess
    ret = subprocess.call(command,shell=True)
    if ret != 0:
        raise ValueError("command failed: %s" % command)
    return ret

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

setup (name = 'python-tarantool17-snapshot',
    description = 'Tarantool 1.6+ snapshot reader',
    version='1.1',
    author='Victor Luchits',
    author_email='vluchits@gmail.com',
    url='https://github.com/viciious/python-tarantool16_snaphot',
    packages=[],
    ext_modules = [module1], **extra_params)

