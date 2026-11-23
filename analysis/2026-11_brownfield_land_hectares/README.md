# Brownfield land: checking the `hectares` field

**Author**: Sian Wilson

**Date created**: 23rd November 2026

**Dataset Scope**: `brownfield-land`

**Purpose**: Supports [digital-land/config#1938](https://github.com/digital-land/config/issues/1938) (Jira: [DLSD-2224](https://mhclgdigital.atlassian.net/browse/DLSD-2224)). A data user reported that some `hectares` values look like square metres. This looks at how widespread that is, and tests the limits proposed on the ticket.

## Outcome

- **Milton Keynes**: 46 values are in square metres, all from one file. Retiring that file should bring back the earlier values, but this hasn't been tested. The council's new `SQM` column shouldn't be mapped to `Hectares`.
- **Other likely errors to check**: Sandwell `7621`, Cheshire West `NOW/0043`, East Cambridgeshire `BLR3120`, Erewash `BLR133` and Thanet `171670`.
- **To raise with councils**: 15 Barnet values of about 0.0009 ha, and values that changed by ×10 in Tamworth (34) and Wolverhampton (34).
- **Limits**: we agree with those on the ticket, with two conditions. The 102 zeros may be better reported as missing, and South Cambridgeshire should confirm Northstowe (`17SC0009`, 1,380.9 ha) before over 1,000 ha becomes an error.

| Hectares | Proposed severity | Entities |
| --- | --- | --- |
| under 0.001 | error | 118 |
| 0.001 to < 0.01 | warning | 127 |
| > 100 to 1,000 | warning | 35 |
| > 1,000 | error | 44 |

## Files

- **[brownfield_land_hectares.ipynb](brownfield_land_hectares.ipynb)**: the full analysis. It downloads live data, so the numbers will change if it's re-run.
