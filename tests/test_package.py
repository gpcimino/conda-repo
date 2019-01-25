import unittest

from condarepo.package import Package

class TestFetch(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        self.url = "https://repo.continuum.io/pkgs/main/"
        self.repodata = {
          "info": {
            "subdir": "win-64"
          },
          "packages": {
            "_ipyw_jlab_nb_ext_conf-0.1.0-py27_0.tar.bz2": {
              "build": "py27_0",
              "build_number": 0,
              "depends": [
                "ipywidgets",
                "jupyterlab",
                "python >=2.7,<2.8.0a0",
                "widgetsnbextension"
              ],
              "license": "BSD",
              "md5": "312cc19649601e9675df7424b5b975a9",
              "name": "_ipyw_jlab_nb_ext_conf",
              "sha256": "823453b06e25e6e7446fc5d30754f046bf14da3087bc21dd994978d7c7ef86e1",
              "size": 3921,
              "subdir": "win-64",
              "timestamp": 1531673202789,
              "version": "0.1.0"
            }
          }
        }

    def test_url(self):
        f = "_ipyw_jlab_nb_ext_conf-0.1.0-py27_0.tar.bz2"
        info = self.repodata['packages'][f]
        p = Package(self.url, f, **info)
        self.assertEqual(self.url +  info['subdir'] + "/" + f, p.url())

    def test_local_filepath(self):
        f = "_ipyw_jlab_nb_ext_conf-0.1.0-py27_0.tar.bz2"
        info = self.repodata['packages'][f]
        p = Package(self.url, f, **info)
        self.assertEqual(p.download_dir() / f, p.local_filepath())


    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        pass
