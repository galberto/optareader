import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="optareader-pkg-galberto", # Replace with your own username
    version="0.0.1",
    author="Gustavo Alberto",
    author_email="galberto.mail@gmail.com",
    description="This package allows to interact with opta files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/galberto/optareader",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'pathlib>=1.0.1',
        'xmltodict==0.12.0',
        'matplotlib==3.2.2',
        'pandas==1.0.5'
        ]

)
