import setuptools

PACKAGE_NAME = "beam_utils"
PACKAGE_VERSION = "0.0.1"

setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    install_requires=[
        # "google-cloud-bigquery==2.34.4",
        "google-cloud-storage==1.44.0",
    ],
    packages=setuptools.find_packages(),
)
