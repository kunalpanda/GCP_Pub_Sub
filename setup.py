import setuptools

setuptools.setup(
    name='weather-preprocessor',
    version='1.0',
    install_requires=['apache-beam[gcp]'],
    packages=setuptools.find_packages(),
)
