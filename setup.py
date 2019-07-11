import setuptools
#     scripts=['src/astor_real_estate.py', 'src/astor_schemas.py', 'sec/astor_housing.py'] ,
with open("README.md", "r") as fh:
    long_description = fh.read()
setuptools.setup(
     name='astor_real_estate',
     version='0.1',
     author="Dean Christakos",
     author_email="dean@astorsquare.com",
     description="Astor Square Real Estate Objects and API",
     long_description=long_description,
   long_description_content_type="text/markdown",
     url="https://github.com/astorsquare/astor_real_estate",
     packages=setuptools.find_packages(),
    install_requires=[                      # Add project dependencies here
        "marshmallow>=2.19.0",
        "marshmallow-jsonapi>=0.21.0",
        "marshmallow-jsonschema>=0.6.0"
    ],
     classifiers=[
         "Programming Language :: Python :: 2",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ],
    entry_points={
        'console_scripts': [
        'astor_real_estate=astor_real_estate:main',
        ],
    },
 )
