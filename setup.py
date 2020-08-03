import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dganalytics",
    version="0.0.1",
    author="Naga Bandarupalli",
    author_email="naga@datagamz.com",
    description="Datagamz Analytics Connectors and Framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://bitbucket.org/Datagamz/dganalytics",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)