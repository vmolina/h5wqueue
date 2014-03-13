from setuptools import setup

setup(name="h5wqueue",
      version="0.1",
      description="Simple utilities for write hdf5 files using workers",
      url='http://github.com/vmolina/h5wqueue',
      author='Victor Bellon Molina',
      author_email='vbellon@gmail.com',
      license='GPLv3',
      packages=['h5wqueue'],
      install_requires=[
          'tables', 'rq'
      ],
)