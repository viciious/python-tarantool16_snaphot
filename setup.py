import os
import sys
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

# for static build with tarantool sources
TARANTOOL_REV = '1.6.8'

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

def download_tarantool_src(path = None):
    import subprocess

    if path is None:
       path = os.path.dirname(__file__)

    src_dir = os.path.join(path,'tarantool_src')
    if not os.path.exists(src_dir):
        sh('git clone https://github.com/tarantool/tarantool.git %s' % src_dir)
        sh('cd %s && git submodule update --init --recursive && git checkout %s && cd -' % (src_dir, TARANTOOL_REV))
    sh("cd %s && cmake ." % src_dir)

    return src_dir


tarantool_src_dir = download_tarantool_src()
sources = ["tarantool_snapshot.cc"]
include_dirs = []
library_dirs = []
extra_compile_args = ["-D__STDC_FORMAT_MACROS", "-D__STDC_LIMIT_MACROS"]
extra_link_args = ["-static-libgcc"]

include_dirs += [
    tarantool_src_dir,
    os.path.join(tarantool_src_dir, "src"),
    os.path.join(tarantool_src_dir, "src", "box"),
    os.path.join(tarantool_src_dir, "src", "lib"),
    os.path.join(tarantool_src_dir, "src", "lib", "msgpuck"),
]
sources += [
    os.path.join(tarantool_src_dir, 'src', 'box', 'iproto_constants.c'),
    os.path.join(tarantool_src_dir, 'src', 'lib', 'msgpuck', 'msgpuck.c'),
]
extra_compile_args += ["-std=c++11"]

module1 = Extension('tarantool16_snapshot',
                    include_dirs = include_dirs,
                    library_dirs = library_dirs,
                    sources = sources,
                    extra_link_args = extra_link_args,
                    extra_compile_args = extra_compile_args)

setup (name = 'python-tarantool16-snapshot',
    description = 'Tarantool 1.6 snapshot reader',
    version='1.1',
    author='Victor Luchits',
    author_email='vluchits@gmail.com',
    url='https://github.com/viciious/python-tarantool16_snaphot',
    packages=[],
    ext_modules = [module1], **extra_params)

