import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
WEBAPP_DIR = ROOT / "webapp"
if str(WEBAPP_DIR) not in sys.path:
    sys.path.insert(0, str(WEBAPP_DIR))

from apps.jobs.tasks import _profile_to_artist_defaults


class JobProfileMappingSmokeTests(unittest.TestCase):
    def test_profile_to_artist_defaults_uses_new_pipeline_shape(self):
        profile = {
            "name": "Artist X",
            "genre": "Jazz",
            "locale": {"city": "Berlin", "country": "Germany"},
            "platforms": {
                "spotify": "https://open.spotify.com/artist/123",
                "youtube": "https://youtube.com/channel/abc",
                "instagram": "https://instagram.com/artistx",
                "website": "https://artistx.example",
            },
            "emails": ["booking@artistx.example"],
            "profile_score": 81,
        }
        job = SimpleNamespace(id="job-1")

        mapped = _profile_to_artist_defaults(profile, job)

        self.assertEqual(mapped["name"], "Artist X")
        self.assertEqual(mapped["city"], "Berlin")
        self.assertEqual(mapped["country"], "Germany")
        self.assertEqual(mapped["spotify_url"], "https://open.spotify.com/artist/123")
        self.assertEqual(mapped["youtube_url"], "https://youtube.com/channel/abc")
        self.assertEqual(mapped["instagram_url"], "https://instagram.com/artistx")
        self.assertEqual(mapped["website_url"], "https://artistx.example")
        self.assertEqual(mapped["email"], "booking@artistx.example")
        self.assertEqual(mapped["profile_score"], 81)
        self.assertIs(mapped["source_job"], job)


if __name__ == "__main__":
    unittest.main()
