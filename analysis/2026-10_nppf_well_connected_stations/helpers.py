"""
Helpers for counting tram, metro and Underground services per hour from the
Traveline National Dataset (TNDS), which is published as regional zips of
TransXChange XML on ftp.tnds.basemap.co.uk.
"""

import collections
import os
import re
import xml.etree.ElementTree as ET
import zipfile
from datetime import date, timedelta
from ftplib import FTP

import pandas as pd

NS = {"t": "http://www.transxchange.org.uk/"}
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
DAY_GROUPS = {
    "MondayToFriday": range(0, 5),
    "MondayToSaturday": range(0, 6),
    "MondayToSunday": range(0, 7),
    "Weekend": range(5, 7),
}

REGIONS = ["EA", "EM", "L", "NE", "NW", "S", "SE", "SW", "W", "WM", "Y"]  # TNDS regions, excluding Isle of Man

# Light rail operators by national operator code. TNDS mode tags are unreliable
# (NET is tagged bus; heritage railways and airport shuttles are tagged tram), so select by operator.
NETWORKS = {
    "METL": "Manchester Metrolink",
    "BLAC": "Blackpool Tramway",
    "NEXT": "Nottingham Express Transit",
    "MDMT": "West Midlands Metro",
    "SYFT": "Sheffield Supertram",
    "TRAM": "London Trams",
    "LDLR": "Docklands Light Railway",
    "LULD": "London Underground",
    "NXMT": "Tyne and Wear Metro",
    "EDTR": "Edinburgh Trams",
    "SPTU": "Glasgow Subway",
}

NAPTAN_LIGHT_RAIL_URL = "https://naptan.api.dft.gov.uk/v1/access-nodes?atcoAreaCodes=940&dataFormat=csv"


def download_regions(user, password, data_dir, regions=REGIONS, refresh=False):
    """Download TNDS regional zips into data_dir, skipping any already there unless refresh."""
    paths = {}
    for region in regions:
        path = os.path.join(data_dir, f"{region}.zip")
        if refresh or not os.path.exists(path):
            with FTP("ftp.tnds.basemap.co.uk") as ftp:
                ftp.login(user, password)
                with open(path, "wb") as f:
                    ftp.retrbinary(f"RETR {region}.zip", f.write)
        paths[region] = path
    return paths


def _duration(s):
    """Parse an ISO 8601 duration such as PT0H3M30S or PT2M into a timedelta."""
    if not s:
        return timedelta()
    h, m, sec = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", s.strip()).groups()
    return timedelta(hours=int(h or 0), minutes=int(m or 0), seconds=int(sec or 0))


def _in_ranges(el, path, day):
    """True if day falls in any DateRange under el/path."""
    for r in el.findall(f"{path}/t:DateRange", NS):
        start = date.fromisoformat(r.findtext("t:StartDate", namespaces=NS))
        end = date.fromisoformat(r.findtext("t:EndDate", default=str(start), namespaces=NS))
        if start <= day <= end:
            return True
    return False


def _runs_on(profile, day):
    """Check an OperatingProfile against a date. Bank holidays are not applied."""
    if profile is None:
        return True
    special = "t:SpecialDaysOperation"
    if _in_ranges(profile, f"{special}/t:DaysOfNonOperation", day):
        return False
    if _in_ranges(profile, f"{special}/t:DaysOfOperation", day):
        return True
    days = profile.find("t:RegularDayType/t:DaysOfWeek", NS)
    if days is None:
        return False  # e.g. HolidaysOnly
    running = set()
    for d in days:
        name = d.tag.split("}")[1]
        if name in WEEKDAYS:
            running.add(WEEKDAYS.index(name))
        elif name in DAY_GROUPS:
            running.update(DAY_GROUPS[name])
    return day.weekday() in running


def parse_txc(xml, day):
    """Return one row per stop call for journeys in a TransXChange file that run on day."""
    day = date.fromisoformat(day) if isinstance(day, str) else day
    root = ET.fromstring(xml)
    service = root.find("t:Services/t:Service", NS)
    start = date.fromisoformat(service.findtext("t:OperatingPeriod/t:StartDate", namespaces=NS))
    end = service.findtext("t:OperatingPeriod/t:EndDate", namespaces=NS)
    if day < start or (end and day > date.fromisoformat(end)):
        return []

    names = {
        s.findtext("t:StopPointRef", namespaces=NS): s.findtext("t:CommonName", namespaces=NS)
        for s in root.findall("t:StopPoints/t:AnnotatedStopPointRef", NS)
    }
    names.update({  # London files use full StopPoint records instead
        s.findtext("t:AtcoCode", namespaces=NS): s.findtext("t:Descriptor/t:CommonName", namespaces=NS)
        for s in root.findall("t:StopPoints/t:StopPoint", NS)
    })
    links = {
        jps.get("id"): jps.findall("t:JourneyPatternTimingLink", NS)
        for jps in root.findall("t:JourneyPatternSections/t:JourneyPatternSection", NS)
    }
    patterns = {
        jp.get("id"): [l for ref in jp.findall("t:JourneyPatternSectionRefs", NS) for l in links[ref.text]]
        for jp in service.findall("t:StandardService/t:JourneyPattern", NS)
    }
    lines = {l.get("id"): l.findtext("t:LineName", namespaces=NS) for l in service.findall("t:Lines/t:Line", NS)}
    service_code = service.findtext("t:ServiceCode", namespaces=NS)
    service_profile = service.find("t:OperatingProfile", NS)

    rows = []
    for vj in root.findall("t:VehicleJourneys/t:VehicleJourney", NS):
        profile = vj.find("t:OperatingProfile", NS)
        if not _runs_on(profile if profile is not None else service_profile, day):
            continue
        h, m, s = map(int, vj.findtext("t:DepartureTime", namespaces=NS).split(":"))
        t = timedelta(hours=h, minutes=m, seconds=s)
        journey = vj.findtext("t:VehicleJourneyCode", namespaces=NS)
        line = lines.get(vj.findtext("t:LineRef", namespaces=NS))
        pattern = patterns[vj.findtext("t:JourneyPatternRef", namespaces=NS)]
        arrival = None
        for i, link in enumerate(pattern):
            frm, to = link.find("t:From", NS), link.find("t:To", NS)
            if i > 0:
                t += _duration(frm.findtext("t:WaitTime", namespaces=NS))
            stop = frm.findtext("t:StopPointRef", namespaces=NS)
            rows.append((service_code, line, journey, stop, names.get(stop), arrival, t))
            t += _duration(link.findtext("t:RunTime", namespaces=NS))
            arrival = t
            t += _duration(to.findtext("t:WaitTime", namespaces=NS))
        stop = to.findtext("t:StopPointRef", namespaces=NS)
        rows.append((service_code, line, journey, stop, names.get(stop), arrival, None))
    return rows


def network_of(xml, name):
    """The light rail network a TransXChange file belongs to, or None."""
    noc = re.search(rb"<NationalOperatorCode>(.*?)</NationalOperatorCode>", xml)
    noc = noc and noc.group(1).decode()
    if noc not in NETWORKS or "Replacement Bus" in name:
        return None
    # Blackpool Transport also runs buses; NET's tram file is the exception tagged as bus
    if b"<Mode>bus</Mode>" in xml and noc != "NEXT":
        return None
    return NETWORKS[noc]


def all_tram_calls(day, data_dir, regions=REGIONS):
    """Stop calls on one date for every light rail network across the given regions' zips in data_dir."""
    all_calls, failed = [], []
    for region in regions:
        zf = zipfile.ZipFile(os.path.join(data_dir, f"{region}.zip"))
        found = collections.Counter()
        for name in zf.namelist():
            if not name.endswith(".xml"):
                continue
            xml = zf.read(name)
            network = network_of(xml, name)
            if network is None:
                continue
            found[network] += 1
            try:
                all_calls += [(region, network, *row) for row in parse_txc(xml, day)]
            except Exception as e:  # formats vary by operator; report rather than stop
                failed.append((region, name, repr(e)))
        print(f"{region}: {dict(found) or 'none'}")
    calls = pd.DataFrame(all_calls, columns=["region", "network", "service_code", "line", "journey",
                                             "stop", "stop_name", "arrival", "departure"])
    # some journeys are published in more than one overlapping file
    calls = calls.drop_duplicates(["network", "line", "stop", "arrival", "departure"])
    return calls, pd.DataFrame(failed, columns=["region", "file", "error"])


def load_light_rail_stations(url=NAPTAN_LIGHT_RAIL_URL):
    """NaPTAN tram, metro and Underground stations: one MET record per station, with coordinates."""
    naptan = pd.read_csv(url, dtype=str)
    stations = naptan[naptan["StopType"] == "MET"]
    return pd.DataFrame({
        "station": stations["ATCOCode"],
        "station_name": stations["CommonName"],
        "latitude": stations["Latitude"].astype(float),
        "longitude": stations["Longitude"].astype(float),
    })


def add_stations(calls, stations):
    """Map each platform to its NaPTAN station and add the station's name and coordinates."""
    # a platform's code is its station's code plus one digit, e.g. 9400ZZSYABR1 -> 9400ZZSYABR
    is_station = calls["stop"].isin(stations["station"])
    calls = calls.assign(station=calls["stop"].where(is_station, calls["stop"].str[:-1]))
    return calls.merge(stations, on="station", how="left")


def trams_per_hour(calls, station):
    """Count departures per hour at a station (all platforms and lines) from add_stations output."""
    at = calls[calls["station"] == station]
    hours = (at["departure"].dropna().dt.total_seconds() // 3600 % 24).astype(int)
    counts = hours.value_counts().sort_index().rename("departures")
    counts.index = counts.index.map(lambda h: f"{h:02d}:00")
    counts.index.name = "hour"
    return counts
