# Endpoint investigations — conclusions & actions

## BIR LBO — dc655cc7187a5959cd573c93279f2c805d4ed680b5c2203d234fa2c703a41318

**Evidence (from regenerated summary):**

| Metric | Value |
|---|---:|
| Organisation | Birmingham City Council |
| Dataset | local-authority |
| Collection | listed-building |
| Pipeline | listed-building-outline |
| First resource start date | 2024-09-24 |
| Last resource start date | 2025-08-13 |
| Total resources | 298 |
| Single-day resources | 280 |
| Single-day resources (%) | 94.0% |
| Days spanned (first → last) | 323 |
| Resources per day (span) | 0.923 |
| Daily for last 7 days | no |
| >20 instances in last 30 days | yes |
| Stale resource (end_date older than 30 days) | no |

After inspecting endpoints URL, I can see that there is the following in the geojson: ENTRY_DATE": "2025-08-14". This appears to be adding the current date instead of the original date of creation. If this is happening each day, then this would explain why a new resource is being created.

---

## BIR TPZ — 2d3cb22e544acf2029c608a945e64943d2bec415ad2ad87aeabed9c7a58a903b

**Evidence (from regenerated summary):**

| Metric | Value |
|---|---:|
| Organisation | Birmingham City Council |
| Dataset | local-authority |
| Collection | tree-preservation-order |
| Pipeline | tree-preservation-zone |
| First resource start date | 2024-09-24 |
| Last resource start date | 2025-08-13 |
| Total resources | 296 |
| Single-day resources | 275 |
| Single-day resources (%) | 92.9% |
| Days spanned (first → last) | 323 |
| Resources per day (span) | 0.916 |
| Daily for last 7 days | yes |
| >20 instances in last 30 days | yes |
| Stale resource (end_date older than 30 days) | no |

After inspecting endpoints URL, I can see that there is the following in the geojson: ENTRY_DATE": "2025-08-14". This appears to be adding the current date instead of the original date of creation. If this is happening each day, then this would explain why a new resource is being created.

---

## LCE CAD — 864d74ebb515466435db25df7b4c936077e745f51b9eca670111cd6e4c202843

**Evidence (from regenerated summary):**

| Metric | Value |
|---|---:|
| Organisation | Leicester City Council |
| Dataset | local-authority |
| Collection | conservation-area |
| Pipeline | conservation-area-document |
| First resource start date | 2024-11-06 |
| Last resource start date | 2025-08-13 |
| Total resources | 277 |
| Single-day resources | 276 |
| Single-day resources (%) | 99.6% |
| Days spanned (first → last) | 280 |
| Resources per day (span) | 0.989 |
| Daily for last 7 days | yes |
| >20 instances in last 30 days | yes |
| Stale resource (end_date older than 30 days) | no |

####### Possible issues:

The two .zip files have different raw hashes:

File 1 hash: cfb92791a47884e624e8d6d2e2d4d0b4c26bf9c4ccc553084c9f6bf7db73390a

File 2 hash: 45f7517f85f0bc5f902dc93fc8b753650efd41568eaeae9d024d3d0e9cc47202

They are not identical, meaning some content or metadata inside the XLSX archives is different, even if the visible spreadsheet data might look the same.

---

## DEFRA AQMA — fc2f0599d4604ae4a9ceeafa994b2583be70bd37552b8fc188b2449d3b2b4700

**Evidence (from regenerated summary):**

| Metric | Value |
|---|---:|
| Organisation | Department for Environment, Food & Rural Affairs |
| Dataset | government-organisation |
| Collection | air-quality-management-area |
| Pipeline | air-quality-management-area |
| First resource start date | 2025-02-04 |
| Last resource start date | 2025-08-13 |
| Total resources | 185 |
| Single-day resources | 181 |
| Single-day resources (%) | 97.8% |
| Days spanned (first → last) | 190 |
| Resources per day (span) | 0.974 |
| Daily for last 7 days | no |
| >20 instances in last 30 days | yes |
| Stale resource (end_date older than 30 days) | no |

####### Possible issues:

Whole ZIP hash: Different → your pipeline will treat them as new resources if it hashes the full ZIP.

Actual shapefile content: .shp, .dbf, .shx, and .prj files are byte-for-byte identical between the two zips.

Differences found: Only in ZIP-level metadata (file modification timestamps, compression metadata, archive order).

---

## WBK CA — 64b205f56014fc81435e61bb55745dbfac6fe7c87dcd13cb391262eb73a8c13b

**Evidence (from regenerated summary):**

| Metric | Value |
|---|---:|
| Organisation | West Berkshire Council |
| Dataset | local-authority |
| Collection | conservation-area |
| Pipeline | conservation-area |
| First resource start date | 2024-01-24 |
| Last resource start date | 2025-08-13 |
| Total resources | 152 |
| Single-day resources | 108 |
| Single-day resources (%) | 71.1% |
| Days spanned (first → last) | 567 |
| Resources per day (span) | 0.268 |
| Daily for last 7 days | no |
| >20 instances in last 30 days | yes |
| Stale resource (end_date older than 30 days) | no |

####### Possible issues:

1. Timestamps

Multiple last_updated and retrieved_at fields differ by date/time, even though the content appears otherwise unchanged.

This alone would cause the system to treat the files as distinct if it uses a full-file hash or metadata-sensitive comparison.

2. Minor ordering differences

In some nested arrays/objects, the order of elements differs slightly. If your pipeline doesn’t normalise ordering before hashing, this would also trigger a “new” resource.

3. No meaningful content changes

Aside from timestamp metadata and ordering, the actual payload (URLs, attributes, data content) is identical.

---

