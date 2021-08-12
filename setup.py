from setuptools import setup, find_packages
from os import path

this_directory = path.abspath(path.dirname(__file__))

try:
    import pypandoc

    long_description = pypandoc.convert_file('README.md', 'rst')
except:
    long_description = ""

requirements = list(map(str.strip, open("requirements.txt").readlines()))

setup(
    name='tairClient',
    version='0.1.3',
    description='a client extends redis.py that gives developers easy access to tair or tairModules',
    author='Cheng Jiang',
    author_email='jiangcheng17@mails.ucas.ac.cn',
    url='https://github.com/631086083/tairClient',
    packages=['tairClient'],
    install_requires=requirements,
    keywords='tair redis tairHash tairString',
    long_description=long_description,
    python_requires=">=3.6"
)
