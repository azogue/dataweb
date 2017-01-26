# -*- coding: utf-8 -*-
"""SETUP de dataweb: Gestión de datos recogidos en web de forma periódica."""
from setuptools import setup, find_packages
from dataweb import __version__ as version


packages = find_packages(exclude=['docs', '*tests*', 'notebooks'])

setup(
    name='dataweb',
    version=version,
    description='Gestión de datos recogidos en web de forma periódica',
    keywords='web scraper',
    author='Eugenio Panadero',
    author_email='azogue.lab@gmail.com',
    url='https://github.com/azogue/dataweb',
    license='MIT',
    packages=packages, install_requires=['numpy', 'pandas', 'requests', 'pytz']
)
