from setuptools import setup, find_packages

setup(
    name="assignment",
    version="0.1",
    packages=find_packages(), install_requires=['pyspark','argparse','unittest'],
    py_modules=['__main__'],
)
