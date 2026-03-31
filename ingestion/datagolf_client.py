import os
import time

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://feeds.datagolf.com"
MASTERS_EVENT_ID = 14
_RATE_LIMIT = 45  # requests per 60 seconds


class DataGolfClient:
    def __init__(self, api_key: str | None = None):
        load_dotenv()
        self.api_key = api_key or os.environ["DATAGOLF_API_KEY"]
        self.session = self._build_session()
        self._req_times: list[float] = []

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        return session

    def _throttle(self) -> None:
        now = time.monotonic()
        self._req_times = [t for t in self._req_times if now - t < 60]
        if len(self._req_times) >= _RATE_LIMIT:
            sleep_for = 60 - (now - self._req_times[0])
            if sleep_for > 0:
                time.sleep(sleep_for)
        self._req_times.append(time.monotonic())

    def _get(self, endpoint: str, params: dict | None = None) -> dict | list:
        self._throttle()
        params = {**(params or {}), "key": self.api_key, "file_format": "json"}
        response = self.session.get(
            f"{BASE_URL}/{endpoint}", params=params, timeout=30
        )
        response.raise_for_status()
        return response.json()

    # --- General endpoints ---

    def get_player_list(self) -> list[dict]:
        """All players who have competed on major tours since 2018."""
        return self._get("get-player-list")

    def get_dg_rankings(self) -> dict:
        """Top 500 players with DG skill estimates and OWGR rank."""
        return self._get("preds/get-dg-rankings")

    def get_skill_ratings(self, display: str = "value") -> dict:
        """Detailed SG skill ratings. display: 'value' or 'rank'."""
        return self._get("preds/skill-ratings", {"display": display})

    def get_approach_skill(self, period: str = "l24") -> dict:
        """Approach skill ratings for a given lookback period."""
        return self._get("preds/approach-skill", {"period": period})

    def get_historical_event_list(self, tour: str = "pga") -> list[dict]:
        """All events available in historical raw data for a given tour."""
        return self._get("historical-raw-data/event-list", {"tour": tour})

    def get_historical_rounds(
        self, event_id: int, year: int, tour: str = "pga"
    ) -> dict:
        """Round-level SG + traditional stats for a given event and year."""
        return self._get(
            "historical-raw-data/rounds",
            {"tour": tour, "event_id": event_id, "year": year},
        )

    def get_historical_event_results(
        self, event_id: int, year: int, tour: str = "pga"
    ) -> dict:
        """Event-level finishes and earnings for a given event and year."""
        return self._get(
            "historical-event-data/events",
            {"tour": tour, "event_id": event_id, "year": year},
        )

    def get_schedule(self, tour: str = "pga") -> dict:
        """Tour schedule (event names, dates, courses). tour: 'pga', 'euro', 'kft', 'liv'."""
        return self._get("get-schedule", {"tour": tour})

    def get_field_updates(self, tour: str = "pga") -> dict:
        """Current field with WDs, tee times, and start holes."""
        return self._get("field-updates", {"tour": tour})

    def get_upcoming_field(self, tour: str = "pga") -> dict:
        """Next week's field (WDs, tee times, start holes). Same schema as get_field_updates()."""
        return self._get("field-updates", {"tour": f"upcoming_{tour}"})

    def get_pre_tournament_predictions(
        self, tour: str = "pga", odds_format: str = "percent"
    ) -> dict:
        """Win/top-5/top-10/top-20/cut probabilities for the current event."""
        return self._get(
            "preds/pre-tournament",
            {"tour": tour, "odds_format": odds_format},
        )

    def get_player_decompositions(self, tour: str = "pga") -> dict:
        """SG prediction breakdown per player for the upcoming event."""
        return self._get("preds/player-decompositions", {"tour": tour})

    # --- Masters convenience methods ---

    def get_masters_rounds(self, year: int) -> dict:
        """Round-level SG data for the Masters (event_id=14) in a given year."""
        return self.get_historical_rounds(MASTERS_EVENT_ID, year)

    def get_masters_results(self, year: int) -> dict:
        """Event-level finishes for the Masters (event_id=14) in a given year."""
        return self.get_historical_event_results(MASTERS_EVENT_ID, year)

    def get_pre_tournament_archive(
        self, event_id: int, year: int, tour: str = "pga", odds_format: str = "percent"
    ) -> dict:
        """Archived pre-tournament predictions for a past event (for back-testing)."""
        return self._get(
            "preds/pre-tournament-archive",
            {"tour": tour, "event_id": event_id, "year": year, "odds_format": odds_format},
        )

    def get_masters_pred_archive(self, year: int) -> dict:
        """Archived pre-tournament predictions for the Masters in a given year."""
        return self.get_pre_tournament_archive(MASTERS_EVENT_ID, year)
