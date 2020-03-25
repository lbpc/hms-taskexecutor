from setuptools import setup, find_packages

setup(
        name='taskexecutor',
        version='0.1.0',
        url='git@gitlab.intr:hms/taskexecutor.git',
        author='Pyotr Sidorov',
        author_email='sidorov@majordomo.ru',
        description='',
        packages=find_packages(),
        install_requires=[],
        test_suite='taskexecutor.test'
)

