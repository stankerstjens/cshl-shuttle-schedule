#!/usr/bin/env python3
from collections import defaultdict
import itertools
from typing import ClassVar, List, Self
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import pandas as pd
from dateutil import tz, parser, utils
import requests
import pymupdf
from icalendar import Calendar, Event

LOCATIONS = {
    "Grace Auditorium": (
        "geo:40.858046,-73.466998",
        {
            "value": "uri",
            "x-apple-mapkit-handle": "CAESmQIIrk0Qg/r9grPK+e6vARoSCSbXCXHUbURAEVHNgknjXVLAImEKDVVuaXRlZCBTdGF0ZXMSAlVTGghOZXcgWW9yayICTlkqDU5hc3NhdSBDb3VudHkyDUxhdXJlbCBIb2xsb3c6BTExNzkxQgtOb3J0aCBTaG9yZYoBC05vcnRoIFNob3JlKhVDU0hMIEdyYWNlIEF1ZGl0b3JpdW0yElN5b3NzZXQsIE5ZICAxMTc5MTINVW5pdGVkIFN0YXRlc1ABWlYKJQiD+v2Cs8r57q8BEhIJJtcJcdRtREARUc2CSeNdUsAYrk2QAwGiHywIg/r9grPK+e6vARofChVDU0hMIEdyYWNlIEF1ZGl0b3JpdW0QACoCZW5AAA==",
            # "x-apple-referenceframe": "0",
            "x-apple-radius": "141.1748962402344",
            "x-title": "Grace Auditorium",
        },
    ),
    "Syosset LIRR Station": (
        "geo:40.824864,-73.500415",
        {
            "value": "uri",
            "x-apple-mapkit-handle": "CAESpAEIrk0Qp+Ha+Jqewc0wGhIJ6dK/JJVpREAROFz1ygZgUsAiOQoNVW5pdGVkIFN0YXRlcxICVVMaCE5ldyBZb3JrIgJOWSoNTmFzc2F1IENvdW50eTIHU3lvc3NldCoPU3lvc3NldCBTdGF0aW9uMgtTeW9zc2V0LCBOWTINVW5pdGVkIFN0YXRlczgvUAFaFQoTCKfh2viansHNMBiuTZADAZgDAQ==",
            # "x-apple-referenceframe": "1",
            "x-apple-radius": "188.604673864844",
            "x-title": "Syosset LIRR Station",
        },
    ),
    "Knight House": (
        "geo:40.861148,-73.462803",
        {
            "value": "uri",
            # "x-apple-referenceframe": "0",
            "x-apple-radius": "141.1748962402344",
            "x-title": "Knight House",
        },
    ),
    "Uplands Farm": (
        "geo:40.857653,-73.453106",
        {
            "value": "uri",
            "x-apple-mapkit-handle": "CAESgAMIrk0QuZHF1ObtsPq4ARoSCaanfpTHbURAEfqH46//XFLAIqEBCg1Vbml0ZWQgU3RhdGVzEgJVUxoITmV3IFlvcmsiAk5ZKg5TdWZmb2xrIENvdW50eTISQ29sZCBTcHJpbmcgSGFyYm9yOgUxMTcyNEILTm9ydGggU2hvcmVSEExhd3JlbmNlIEhpbGwgUmRaAzI1MGIUMjUwIExhd3JlbmNlIEhpbGwgUmRyC0xvbmcgSXNsYW5kigELTm9ydGggU2hvcmUqFlVwbGFuZHMgRmFybSBTYW5jdHVhcnkyFDI1MCBMYXdyZW5jZSBIaWxsIFJkMh1Db2xkIFNwcmluZyBIYXJib3IsIE5ZICAxMTcyNDINVW5pdGVkIFN0YXRlc1ABWloKKAi5kcXU5u2w+rgBEhIJpqd+lMdtREAR+ofjr/9cUsAYrk2QAwGYAwGiHy0IuZHF1ObtsPq4ARogChZVcGxhbmRzIEZhcm0gU2FuY3R1YXJ5EAAqAmVuQAA=",
            # "x-apple-referenceframe": "1",
            "x-apple-radius": "188.604673864844",
            "x-title": "Uplands Farm",
        },
    ),
    "Woodbury": (
        "geo:40.801248,-73.467998",
        {
            "value": "uri",
            "x-apple-mapkit-handle": "CAES/gIIrk0Q u8vw162PvuAUGhIJCryTT49mREARj+W4rvNdUsAikgEKDVVuaXRlZCBTdGF0ZXMSAlVTGghOZXcgWW9yayICTlkqDU5hc3NhdSBDb3VudHkyCFdvb2RidXJ5OgUxMTc5N0ILTm9ydGggU2hvcmVSDlN1bm55c2lkZSBCbHZkWgM1MDBiEjUwMCBTdW5ueXNpZGUgQmx2ZHILTG9uZyBJc2xhbmSKAQtOb3J0aCBTaG9yZSojQ29sZCBTcHJpbmcgSGFyYm9yIExhYm9yYXRvcnkgUHJlc3MyEjUwMCBTdW5ueXNpZGUgQmx2ZDITV29vZGJ1cnksIE5ZICAxMTc5NzINVW5pdGVkIFN0YXRlczgvUAFaZQonCLvL8Netj77gFBISCQq8k0+PZkRAEY/luK7zXVLAGK5NkAMBmAMBoh85CLvL8Netj77gFBotCiNDb2xkIFNwcmluZyBIYXJib3IgTGFib3JhdG9yeSBQcmVzcxAAKgJlbkAA",
            "x-apple-referenceframe": "1",
            "x-apple-radius": "225.3715646891437",
            "x-title": "Woodbury",
        },
    ),
}


PDF_URL = (
    "https://www.cshl.edu/wp-content/uploads/2025/07/Shuttle-Schedule_06.01.2025.pdf"
)
TIMEZONE = "America/New_York"


def fetch_pdf(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


def get_schedule_doc(url: str) -> pymupdf.Document:
    return pymupdf.open(stream=fetch_pdf(url))


def to_pandas_table(table) -> pd.DataFrame:
    df = table.to_pandas()
    df.replace(r"[\n\r]", " ", regex=True, inplace=True)
    df.columns = df.columns.str.replace(r"[\n\r]", " ", regex=True)
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    return df


TIME_ZONE = tz.gettz("America/New_York")
ROUTES = []
PAIRS = []
ALL_STOPS = []
EVENT_DURATION = timedelta(minutes=5)


@dataclass(frozen=True)
class Stop:
    time: datetime
    name: str

    @property
    def location(self):
        return " ".join(self.name.split(" ")[2:])

    @property
    def departing(self):
        return "Departs" in self.name

    @property
    def arriving(self):
        return "Arrives" in self.name


@dataclass
class Route:
    shuttle: str
    stops: List[Stop] = field(default_factory=list)

    all: ClassVar[list[Self]] = []

    def __post_init__(self):
        Route.all.append(self)

    @property
    def description(self) -> str:
        return f"{self.shuttle}\n" + "\n".join(
            f"{d.time.strftime(r'%H:%M')} {d.name}" for d in self.stops
        )

    def __del__(self):
        Route.all.remove(self)


def parse_time(time) -> datetime | None:
    try:
        return utils.default_tzinfo(parser.parse(time), TIME_ZONE)  # type: ignore
    except (TypeError, parser.ParserError):
        return None


def parse_table(table):
    titles = table.columns.tolist()[1:]

    for _, row in table.iterrows():
        try:
            shuttle_name, *times = row
        except ValueError:
            continue

        route = Route(
            shuttle=shuttle_name,
            stops=[
                Stop(t, name)
                for t, name in zip(map(parse_time, times), titles)
                if t is not None
            ],
        )
        if len(route.stops) <= 1:
            del route


def create_calendar(f, t):
    cal = Calendar()
    cal.add("prodid", f"-//CSHL Shuttle//Schedule//EN")
    cal.add("version", "2.0")
    cal.add("X-WR-CALNAME", f"CSHL Shuttle: {f.split(' ')[0]}-{t.split(' ')[0]}")
    return cal


def export_calendars():

    calendars = {}
    events = defaultdict(set)

    for route in Route.all:
        to_skip = set()
        for fr, to in itertools.pairwise(route.stops):
            if fr.location == to.location and fr.arriving and to.departing:
                to_skip.add(fr)

        for i, fr in enumerate(route.stops):
            if fr in to_skip:
                continue

            event_key = (fr.location, fr.time)

            event = Event()
            event.add("dtstart", fr.time)
            event.add("duration", EVENT_DURATION)
            event.add("summary", f"{route.shuttle}: {fr.location}")
            event.add("description", route.description)
            event.add("rrule", "freq=daily;byday=mo,tu,we,th,fr")
            event.add("location", fr.location)

            if fr.location in LOCATIONS:
                geo, params = LOCATIONS[fr.location]
                event.add(
                    "x-apple-structured-location",
                    geo,
                    parameters=params,
                )

            for to in [*route.stops[i + 1 :], route.stops[0]]:
                f, t = fr.location, to.location

                # It really is about the location: it does not serve as an arbitrary
                # identifier for the stop.
                if f != t:
                    cal_key = (f, t) if f < t else (t, f)

                    if cal_key not in calendars:
                        calendars[cal_key] = create_calendar(f, t)

                    if event_key not in events[cal_key]:
                        calendars[cal_key].add_component(event)
                        events[cal_key].add(event_key)

    for (fr, to), calendar in calendars.items():
        with open(f"CSHL_Shuttle-{fr}-{to}.ics", "wb") as f:
            f.write(calendar.to_ical())


def extract_table(doc: pymupdf.Document):
    # The schedule should be a single page

    if not doc or len(doc) != 1:
        raise ValueError("Expected a single-page PDF document.")
    page: pymupdf.Page = doc[0]  # Get the first page

    weekday_table, weekend_table = map(
        to_pandas_table,
        page.find_tables(  # type: ignore
            strategy="lines_strict",
        ),
    )

    parse_table(weekday_table)
    weekday_table.to_csv(f"weekday_schedule.tsv", sep="\t", index=False)


if __name__ == "__main__":

    doc = get_schedule_doc(PDF_URL)
    extract_table(doc)
    export_calendars()
