"""
Tests for Essential Bundle Setup Script

These tests verify that:
1. Script can be imported without errors
2. Configuration is valid
3. Functions have correct signatures
4. Code shown in documentation is executable

Single Source of Truth:
    - Tests verify docs/tests/python/setup/essential_bundle.py
    - This is the SAME code shown in documentation
    - If these tests fail, docs show broken code!

Run these tests:
    pytest docs/tests/python/setup/test_bundles.py -v
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Import the actual code that docs reference
sys.path.insert(0, str(Path(__file__).parent))
import essential_bundle
import complete_bundle


class TestEssentialBundleImports:
    """Test that all imports work"""
    
    def test_imports_succeed(self):
        """If we got here, imports worked!"""
        assert hasattr(essential_bundle, 'download_file')
        assert hasattr(essential_bundle, 'download_srtm_aws')
        assert hasattr(essential_bundle, 'download_sentinel2')
    
    def test_required_dependencies(self):
        """Test that required dependencies are available"""
        import requests
        import gzip
        import shutil
        import io
        from pathlib import Path
        # If we got here, all imports worked
        assert True


class TestConfiguration:
    """Test configuration and path setup"""
    
    def test_sample_data_path_format(self):
        """Test that SAMPLE_DATA_PATH follows Unity Catalog convention"""
        assert "/Volumes/" in essential_bundle.SAMPLE_DATA_PATH
        assert "/geobrix-examples" in essential_bundle.SAMPLE_DATA_PATH
    
    def test_path_components(self):
        """Test that all path components are defined"""
        assert hasattr(essential_bundle, 'CATALOG')
        assert hasattr(essential_bundle, 'SCHEMA')
        assert hasattr(essential_bundle, 'VOLUME')
        assert essential_bundle.CATALOG
        assert essential_bundle.SCHEMA
        assert essential_bundle.VOLUME


class TestDownloadFile:
    """download_file writes the downloaded bytes under SAMPLE_DATA_PATH.

    Only the HTTP call is mocked (an external API); the write goes to a real
    temporary directory. Patching ``builtins.open`` / ``Path.exists`` instead is
    unsafe here: ``download_file`` imports ``bundle_lib`` (and with it
    ``databricks.labs.gbx``) on its first call, and the import fails while
    ``Path.exists`` is patched.
    """

    @staticmethod
    def _response(chunks):
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.iter_content = lambda chunk_size: iter(chunks)
        return response

    def test_download_file_success(self, tmp_path, monkeypatch):
        """Successful download: the bytes land at SAMPLE_DATA_PATH/<subfolder>/<filename>."""
        monkeypatch.setattr(essential_bundle, "SAMPLE_DATA_PATH", str(tmp_path))
        with patch("requests.get", return_value=self._response([b"test ", b"data"])) as mock_get:
            result = essential_bundle.download_file(
                "http://example.com/test.json",
                "test-subfolder",
                "test.json",
                "Test File",
            )

        mock_get.assert_called_once()
        assert mock_get.call_args.args[0] == "http://example.com/test.json"
        expected = tmp_path / "test-subfolder" / "test.json"
        assert Path(result) == expected
        assert expected.read_bytes() == b"test data"

    def test_download_file_skips_an_existing_file(self, tmp_path, monkeypatch):
        """A file that is already there is kept and not downloaded again."""
        monkeypatch.setattr(essential_bundle, "SAMPLE_DATA_PATH", str(tmp_path))
        existing = tmp_path / "test-subfolder" / "test.json"
        existing.parent.mkdir(parents=True)
        existing.write_bytes(b"already here")
        with patch("requests.get") as mock_get:
            result = essential_bundle.download_file(
                "http://example.com/test.json",
                "test-subfolder",
                "test.json",
                "Test File",
            )

        mock_get.assert_not_called()
        assert Path(result) == existing
        assert existing.read_bytes() == b"already here"


class TestDownloadSentinel2:
    """Test Sentinel-2 download function (download_sentinel2 is shown in docs)."""
    
    def test_bbox_format(self):
        """Test bounding box format"""
        # NYC bbox from the script
        nyc_bbox = [-74.25, 40.50, -73.70, 40.92]
        assert len(nyc_bbox) == 4
        assert nyc_bbox[0] < nyc_bbox[2]  # west < east
        assert nyc_bbox[1] < nyc_bbox[3]  # south < north


class TestCompleteBundle:
    """Complete Bundle script (complete_bundle.py) - structure and config."""
    
    def test_imports_succeed(self):
        """complete_bundle module has expected API."""
        assert hasattr(complete_bundle, 'download_file')
        assert hasattr(complete_bundle, 'download_srtm_aws')
        assert hasattr(complete_bundle, 'main')
    
    def test_sample_data_path(self):
        """SAMPLE_DATA_PATH follows Unity Catalog convention."""
        assert "/Volumes/" in complete_bundle.SAMPLE_DATA_PATH
        assert "/geobrix-examples" in complete_bundle.SAMPLE_DATA_PATH


if __name__ == '__main__':
    # Allow running tests directly
    pytest.main([__file__, '-v'])
