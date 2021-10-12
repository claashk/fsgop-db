#!/usr/bin/env python3

from setuptools import setup, find_namespace_packages
from pathlib import Path


def read(path):
    with open(path) as input_file:
        return input_file.read()


name = "fsgop.db"

version_file = Path(__file__).parent / Path(*name.split('.')) / "version.py"
version_file_vars = {}
exec(read(version_file), version_file_vars)
release = version_file_vars['version']
version = ".".join(release.split('.')[:2])

try:
    from sphinx.setup_command import BuildDoc
    build_doc = {
        'cmdclass': {'build_sphinx': BuildDoc},
        'command_options': {
            'build_sphinx': {
                'project': ('setup.py', name),
                'version': ('setup.py', version),
                'release': ('setup.py', release),
                'source_dir': ('setup.py', 'doc/source')
            }
        }
    }
except ImportError:
    build_doc={}


setup(
    name=name,
    version=release,
    packages=find_namespace_packages(include=['fsgop.*']),
    scripts=[],
    install_requires=[ # See https://packaging.python.org/discussions/install-requires-vs-requirements/
        'setuptools',
        'sphinx',
        'sphinx-rtd-theme',
        'mysqlclient',
        'numpy'
    ],
    author='TBD',
    author_email='none',
    description='database tools',
    long_description = read('README.md'),
    long_description_content_type="text/markdown",
    keywords='csv database',
    url='https://github.com/TBD',
    project_urls={'Repository': 'https://github.com/TBD'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta"
    ],
    test_suite='tests',
    python_requires=">=3.6",
    **build_doc
)
