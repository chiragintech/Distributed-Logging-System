from setuptools import setup, find_packages

setup(
    name="distributed_logger",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'flask',
        'requests',
        'fluent-logger',
        'networkx',
        'elasticsearch',
        'kafka-python',
        'colorama',
        'opentelemetry-api',
        'opentelemetry-sdk',
        'opentelemetry-instrumentation-flask',
        'opentelemetry-instrumentation-requests'
    ],
    python_requires='>=3.8'
)