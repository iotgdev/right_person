import os
from setuptools import setup, find_packages

from spark_data_miner.cluster.ami.constants import PACKAGE_DEPENDENCIES

about = {
    'here': os.path.abspath(os.path.dirname(__file__))
}

with open(os.path.join(about['here'], 'version.py')) as f:
    exec(f.read(), about)

try:
    with open(os.path.join(about['here'], 'test', '__init__.py')) as f:
        exec(f.read(), about)
except IOError:
    pass

with open(os.path.join(about['here'], 'README.md')) as f:
    about['readme'] = f.read()


setup(
    # available in PKG-INFO
    name='right-person',
    version=about['__version__'],
    description='Targeted audience machine learning for Realtime Bidding',
    url='https://github.com/iotgdev/right_person/',
    author='iotec',
    author_email='dev@dsp.io',
    license='MIT',
    download_url='https://github.com/iotgdev/right_person/archive/{}.tar.gz'.format(about['__version__']),
    long_description=about['readme'],
    platforms=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
    ],

    # Package Properties
    packages=find_packages(include=['right_person', 'right_person.*', 'spark_data_miner', 'spark_data_miner.*']),
    include_package_data=True,
    test_suite='test',
    setup_requires=['pytest-runner'],
    tests_require=['mock>=2.0.0', 'pytest'],
    cmdclass={'pytest': about.get('PyTest')},
    entry_points={
        'console_scripts': [
            'build_right_person_ami=spark_data_miner.cluster.ami.utils:create_ami_from_instance'
        ]
    },
    install_requires=[
    ] + PACKAGE_DEPENDENCIES
)
