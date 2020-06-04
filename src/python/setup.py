from setuptools import setup, find_packages

setup(
        name='taskexecutor',
        version='0.2.0',
        url='git@gitlab.intr:hms/taskexecutor.git',
        author='Pyotr Sidorov',
        author_email='sidorov@majordomo.ru',
        description='Operational tasks manager and executor for HMS',
        packages=find_packages(),
        test_suite='test'
)

