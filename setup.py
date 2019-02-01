import setuptools

if __name__ == '__main__':
    with open("requirements.txt") as rf:
        setuptools.setup(
            name='condarepo',
            version='0.1',
            packages=['condarepo'],
            include_package_data=True,

            # This automatically detects the packages in the specified
            # (or current directory if no directory is given).
            #packages=setuptools.find_packages(),
            zip_safe=False,


            author='Giampaolo Cimino',
            author_email='gcimino@gmail.com',

            description='Simple mirroring script for Anaconda package repository',


            long_description='''Simple mirroring script for Anaconda package repository with full multi-process support''',

            # The license should be one of the standard open source
            # licenses: https://opensource.org/licenses/alphabetical
            license='Apache-2.0',

            install_requires=rf.readlines(),
            classifiers=[
                'Development Status :: 3 - Alpha',

                # Indicate who your project is intended for
                'Intended Audience :: System Administrators',
                'Topic :: System :: Software Distribution',

                'Programming Language :: Python :: 3.5',
                'Programming Language :: Python :: 3.6',
            ],          
            keywords='python anaconda conda repository mirror multi-process parallel',
            entry_points={
                'console_scripts': [
                    'condarepo = condarepo.main:main',

                ]
            }

        )