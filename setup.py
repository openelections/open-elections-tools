from setuptools import setup, find_packages

VERSION = '0.0.1'

setup(name='open-elections',
      version=VERSION,
      packages=find_packages(),
      install_requires=['pandas>=1.0.5',
                        'doltpy>=1.0.10'],
      tests_require=['pytest'],
      setup_requires=['wheel'],
      author='Open Elections',
      author_email='oscar@liquidata.co',
      description='A Python package for working with Open Elections data',
      entry_points={
          'console_scripts': ['validate-state=open_elections.validation.data_issues_by_state:main']
      }
  )