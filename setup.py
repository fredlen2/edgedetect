from setuptools import setup, find_packages
# import os, sys


def parse_requirements(requirements):
    # load from requirements.txt
    with open(requirements) as f:
        lines = [l for l in f]
        # remove spaces
        stripped = list(map((lambda x: x.strip()), lines))
        # remove comments
        nocomments = list(filter((lambda x: not x.startswith('#')), stripped))
        # remove empty lines
        reqs = list(filter((lambda x: x), nocomments))
        return reqs


PACKAGE_NAME = "Tramscore"
PACKAGE_VERSION = "0.1.0"
SUMMARY = 'Tramscore: Audio Fingerprinting in Python'
DESCRIPTION = """
Audio fingerprinting and recognition algorithm implemented in Python

# Original author is'Will Drevo',
# author_email='will.drevo@gmail.com',

Tramscore can memorize recorded audio by listening to it once and fingerprinting 
it. Then by playing an advert and recording microphone input or on disk file, 
Tramscore attempts to match the audio against the fingerprints held in the 
database, returning the advert or recording being played.

"""
REQUIREMENTS = parse_requirements("requirements.txt")

setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description=SUMMARY,
    long_description=DESCRIPTION,
    author='Fred Kwadwo Aazore',
    author_email='fredaazore@gmail.com',
    maintainer="Fred Kwadwo Aazore",
    maintainer_email="fredaazore@gmail.com",
    url='http://github.com/fredlen2/tramscore',
    license='MIT License',
    include_package_data=True,
    packages=find_packages(),
    platforms=['Unix'],
    install_requires=REQUIREMENTS,
    classifiers=[
        'Development Status :: 1 - Beta',
        'Environment :: Console',
        'Intended Audience :: Advertisers, Media monitoring houses',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    keywords="trams, python, audio, fingerprinting, music, numpy, landmark",
)
