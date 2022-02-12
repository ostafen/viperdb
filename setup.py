import pathlib
from setuptools import setup

HERE = pathlib.Path(__file__).parent

README = (HERE / "README.md").read_text()

setup(
    name="viperdb",
    version="1.0.0",
    description="A tiny log-structured key-value database written in pure Python",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/ostafen/viperdb",
    author="Real Python",
    author_email="stefano.scafiti96@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
    packages=["viperdb"],
    include_package_data=True,
)
