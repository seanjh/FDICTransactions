try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='fdic_trans',
    version='0.1',
    packages=['storage', 'scrape'],
    url='',
    license='',
    author='Sean Herman',
    author_email='seanherman@gmail.com',
    description='',
    requires=['requests>=2.2', 'lxml>=3.3', 'sqlalchemy>=0.9', 'pyodbc']
)
