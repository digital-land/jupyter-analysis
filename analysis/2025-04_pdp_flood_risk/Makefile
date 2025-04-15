
all: make_dirs extract_samples calculate_overlap

make_dirs:
	@mkdir -p processed output

extract_samples:
	@echo "extracting samples from flood risk zone files"
	duckdb :memory: < 01_extract_sample.sql

calculate_overlap:
	@echo "calculating polygon overlaps between samples"
	duckdb :memory: < 02_calc_overlap.sql