import os
from setuptools import setup, find_packages


about = {
    'here': os.path.abspath(os.path.dirname(__file__))
}

with open(os.path.join(about['here'], 'version.py')) as f:
    exec (f.read(), about)

try:
    with open(os.path.join(about['here'], 'test', '__init__.py')) as f:
        exec (f.read(), about)
except IOError:
    pass

with open(os.path.join(about['here'], 'README.md')) as f:
    about['readme'] = f.read()


setup(
    # available in PKG-INFO
    name='right_person',
    version=about['__version__'],
    description='Cross Customer Model machine learning for Realtime Bidding',
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
    ],

    # Package Properties
    packages=find_packages(include=['right_person', 'right_person.*']),
    include_package_data=True,
    test_suite='test',
    setup_requires=['pytest-runner'],
    tests_require=['mock>=2.0.0', 'pytest'],
    cmdclass={'pytest': about.get('PyTest')},
    scripts=['bin/right_person_cluster_manager'],
    install_requires=[
        'boto3',
        'mmh3',
        'numpy',
        'pyspark==2.3.2',
        'requests',
        'scipy==1.1.0',
        'scikit-learn',
        'ujson',
        'ulid',
    ]
)
