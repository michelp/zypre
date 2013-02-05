from setuptools import setup

with open('VERSION', 'rb') as version_file:
    version = version_file.read().strip()

setup(name='zypre',
      version=version,
      description='Python ZRE protocol implementation',
      long_description=" ".join("""
        ...
      """.split()),
      author='Michel Pelletier',
      author_email='pelletier.michel@yahoo.com',
      packages=['zypre'],
      include_package_data=True,
      install_requires="""
        pyzmq
        gevent
        """,

      keywords="python zeromq 0mq pyzmq gevent distributed",
      classifiers=[
        'Development Status :: 3 - Alpha',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries',
        'Topic :: Utilities'])
