from setuptools import setup, find_packages

setup(
    name="feature_platform",
    version="0.1.0",
    packages=find_packages(include=['domain', 'runner']),
    install_requires=[
        'pyspark>=3.0.0',
        'pyyaml>=5.4.1',
        'pandas>=1.3.0',
        'numpy>=1.21.0',
        'delta-spark>=1.0.0',
    ],
    python_requires='>=3.8',
    author="Your Name",
    author_email="your.email@example.com",
    description="Feature Platform for Databricks",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/feature-platform",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
