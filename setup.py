from setuptools import setup
 
setup(
    name = 'pydynamo',
    packages = ['pydynamo'],
    py_modules = ['pydynamo'],
    version = '0.1.3',
    description = 'Python library for communicating with dynamo db !',
    author='Nidhi Mittal',
    author_email='mittalnidhi.mittal@gmail.com',
    url='http://github.com/nidhi-delhivery/pydynamo',
    license='LICENSE.txt',
    include_package_data=True,
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: OS Independent',
        'Development Status :: 1 - Planning',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: GIS'
    ],
    install_requires=[
        "Django >= 1.4",
        "boto == 2.2",
    ],
)
