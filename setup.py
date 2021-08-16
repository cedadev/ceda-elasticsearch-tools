"""A setuptools based setup module.

See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='ceda-elasticsearch-tools',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html

    version='2.3.2',

    description='Extension of the basic elasticsearch python wrapper to perform operations with a given ES index.',
    long_description=long_description,

    # The project's main homepage.
    url='https://github.com/cedadev/ceda-elasticsearch-tools.git',

    # Author details
    author='Richard Smith',
    author_email='richard.d.smith@stfc.ac.uk',

    # Choose your license
    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 5 - Production',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.

        'Programming Language :: Python :: 3.5',
    ],

    # What does your project relate to?
    keywords='elasticsearch',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    package_data={
        'ceda_elasticsearch_tools': ['root_certificate/root-ca.pem']
    },

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=[
        'requests',
        'elasticsearch<=8.0.0',
        'docopt'
    ],

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points={
        'console_scripts': [
            'nla_sync_es.py=ceda_elasticsearch_tools.cmdline.nla_sync_es:main',
            'nla_sync_lotus_task.py=ceda_elasticsearch_tools.cmdline.secondary_scripts.nla_sync_lotus_task:main',
            'update_md5.py=ceda_elasticsearch_tools.cmdline.update_md5:main',
            'md5.py=ceda_elasticsearch_tools.cmdline.secondary_scripts.md5:main',
            'fbs_missing_files.py=ceda_elasticsearch_tools.cmdline.fbs_missing_files:main',
            'spot_checker.py=ceda_elasticsearch_tools.cmdline.secondary_scripts.spot_checker:main',
            'fbs_live_index=ceda_elasticsearch_tools.cmdline.fbs_live_index:main',
            'coverage_test=ceda_elasticsearch_tools.cmdline.ceda_eo.coverage_test:main'
        ],
    },

)
