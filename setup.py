from setuptools import setup

setup(
    name='compact',
    version='0.1',
    py_modules=['compact'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        compact=compact:cli
    ''',
)
