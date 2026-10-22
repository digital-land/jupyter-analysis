# NPPF well-connected stations: service frequency

**Author**: Sian Wilson

**Date created**: October 2026

**Dataset Scope**: National Rail, tram, metro and Underground stations in Great Britain

**Purpose**: Supports [digital-land/config#3035](https://github.com/digital-land/config/issues/3035). For the NPPF work, we need to identify "well-connected" stations, which depends on how often services call there. This folder tests whether that's feasible from open timetable data, by counting services per hour at every station on a chosen date. It may lead to a new dataset on the Planning Data platform.

The work is split by data source: National Rail comes from Network Rail's timetable, and trams, metros and the Underground come from the Traveline National Dataset (TNDS). Both return counts per hour per station, joined to NaPTAN for station names and coordinates.

## `railway_stations.ipynb`

Counts National Rail trains per hour at a station.

- Downloads the full GB rail timetable (SCHEDULE feed, CIF format) from the Network Rail Open Data service. This needs a free account.
- Filters it to permanent (STP = `P`) schedules, and to stops with a public (GBTT) arrival or departure time.
- Keeps origin (`LO`) and terminating (`LT`) records as well as intermediate (`LI`) ones. Without them, termini such as Brighton show almost no trains, because nearly all their trains start or end there.
- Checks each schedule's date range and days-run against the query date, then counts arrivals and departures by hour.
- Joins to NaPTAN rail stations (area code `910`) on TIPLOC. About 96% of stops match.

**Still to do**: filter to passenger categories (`OO`/`XX`), since the feed also contains buses, ferries and freight; and apply overlay and cancellation schedules if the actual service on a date is needed, not the planned base timetable.

## `tram_stations.ipynb`

Counts tram, metro and Underground departures per hour at a station, for 11 networks: Manchester Metrolink, Blackpool Tramway, Nottingham Express Transit, West Midlands Metro, Sheffield Supertram, Edinburgh Trams, London Trams, DLR, London Underground, Tyne and Wear Metro and Glasgow Subway. That's about 720 stations.

- Downloads the TNDS regional zips (TransXChange XML) over FTP. This needs a free Traveline account.
- Has a regional section for exploring one region, and a national section that combines all regions.
- Selects networks by operator code, not by TNDS's mode tag. The tags are unreliable: Nottingham's tram is tagged as a bus, and heritage railways and airport shuttles are tagged as trams.
- Works out each journey's time at every stop from its start time and the run times between stops, then removes duplicate journeys. Some operators publish the same journeys in more than one overlapping file.
- Groups platforms into stations using NaPTAN (area code `940`), where a platform's code is its station's code plus one digit. Every stop matched.

**Caveats**: bank holidays aren't applied, and planned closures change the service (e.g. Metrolink's city centre closure, 19 Oct to 1 Nov 2026), so use an ordinary date. Counts combine both directions.

## `helpers.py`

The functions used by `tram_stations.ipynb`: the TransXChange parser, the list of networks (`NETWORKS`), the FTP download, the NaPTAN station matching and `trams_per_hour`. To add a network, add its national operator code to `NETWORKS`.

## Data sources

- [Network Rail Open Data](https://publicdatafeeds.networkrail.co.uk/): SCHEDULE feed, `CIF_ALL_FULL_DAILY_toc-full.CIF.gz`. The full extract is refreshed weekly, on Fridays. The record format is documented on the [Open Rail Data Wiki](https://wiki.openraildata.com/index.php/CIF_File_Format).
- [Traveline National Dataset](https://www.travelinedata.org.uk/traveline-open-data/traveline-national-dataset/): regional zips on `ftp.tnds.basemap.co.uk`, refreshed weekly.
- [DfT NaPTAN API](https://naptan.api.dft.gov.uk/): rail stations (area `910`) and tram/metro stops (area `940`). No account needed.

Downloads and outputs are written to the repo's top-level `data/` directory (gitignored), not committed here: the CIF file and its filtered `out.csv`, and the TNDS regional zips (about 1.45 GB in total).
