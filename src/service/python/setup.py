from distutils.core import setup

setup(
    name = 'derecho.cascade',
    version = '1.0rc1',
    description = 'The Cascade Python API',
    url = 'https://github.com/derecho-project/cascade',
    author = 'Weijia Song and Aahil Awatramani',
    author_email = 'songweijia@gmail.com',
    license = 'BSD 3-Clause "New" or "Revised" License',
    zip_safe = True,
    packages = ['derecho','derecho.cascade'],
    package_data = {'derecho.cascade' : ['client.cpython-38-x86_64-linux-gnu.so']},
    python_requires = ">=3.8"
)
