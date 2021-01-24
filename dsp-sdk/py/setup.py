import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dspsdk",
    version="2.1.0",
    author="Ketan Patil",
    author_email="dsp-engg@flipkart.com",
    description="A small example package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.fkinternal.com/dsp",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "requests >= 2.23.0",
        "hdfs >= 2.5.8",
        "python-hostsresolver >= 0.0.4",
        "pandas",
        "pyyaml"
    ],
    python_requires='>=3.6',
)