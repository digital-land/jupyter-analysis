
# Description
Small bit of analysis comparing records between flood risk zone dataset releases, in order to inform decisions on how to manage the relationship between them on the platform.

## 2023 data
Two flood map for planning datasets:
* [flood risk zone 2](https://environment.data.gov.uk/dataset/86ec354f-d465-11e4-b09e-f0def148f590), captured as resource [124a62c973429e80bb59ded0f049f5237ad8c6906edb4c73ac5749761febca79](https://datasette.planning.data.gov.uk/digital-land/resource/124a62c973429e80bb59ded0f049f5237ad8c6906edb4c73ac5749761febca79).
* [flood risk zone 3](https://environment.data.gov.uk/dataset/87446770-d465-11e4-b97a-f0def148f590), captured as resource [e520b03633366cdca46708b9a98809417fe79e4d6b16e07adfb12e00888c38f9](https://datasette.planning.data.gov.uk/digital-land/resource/e520b03633366cdca46708b9a98809417fe79e4d6b16e07adfb12e00888c38f9).

Number of records:
* Zone 2 = 550,621
* Zone 3 = 230,015

## 2025 data
One [single dataset](https://environment.data.gov.uk/dataset/04532375-a198-476e-985e-0579a0a11b47), which has a field `Flood_Zone` to distinguish the zones.

Number of records:
* Zone 2 = 11,178,451
* Zone 3 = 1,625,675

## Record comparisons
There are no comparable references or other fields between the datasets, so we're testing whether any polygons match between them, which would suggest they represent the same entity and entities from the 2023 data can be re-used where possible.

Because of the large size, we're testing with a sample taken by extracting records from each dataset which are within an area.

### Bath sample area
Bath test area: `POLYGON ((-2.367897 51.363421, -2.282753 51.363421, -2.282753 51.40124, -2.367897 51.40124, -2.367897 51.363421))`  
[[see area on wktmap.com](https://wktmap.com/?53a2c5f0)]

2023 records, summary statistics of polygon area (calculated in Km^2):
```
count    41.000000
mean      0.317394
std       1.524481
min       0.000000
25%       0.000020
50%       0.000639
75%       0.019566
max       9.539646
```

2025 records, summary statistics of polygon area (calculated in Km^2):
```
count    1228.000000
mean        0.000633
std         0.007144
min         0.000003
25%         0.000004
50%         0.000008
75%         0.000024
max         0.204176
```

Number of good matches (> 90% shared area) between datasets = 4   
4 / 41 (9.76%) 2023 records matched to 4 / 1228 (0.33%) 2025 records