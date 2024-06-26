import csv
from cchardet import UniversalDetector
import logging
import os
import os.path
import sqlite3
import subprocess
import tempfile
import zipfile
from io import StringIO
import pandas as pd
from digital_land.phase.load import Stream
from digital_land.phase.phase import Phase
from digital_land.log import DatasetResourceLog
import urllib.request
import json_stream


def detect_file_encoding(path):
    with open(path, "rb") as f:
        return detect_encoding(f)


def detect_encoding(f):
    detector = UniversalDetector()
    detector.reset()
    for line in f:
        detector.feed(line)
        if detector.done:
            break
    detector.close()
    return detector.result["encoding"]


def load_csv(path, encoding="UTF-8", log=None):
    logging.debug(f"trying csv {path}")

    if not encoding:
        encoding = detect_file_encoding(path)

        if not encoding:
            return None

        logging.debug(f"detected encoding {encoding}")

    f = open(path, encoding=encoding, newline=None)
    content = f.read()
    if content.lower().startswith("<!doctype "):
        logging.debug(f"{path} has <!doctype")
        return None

    f.seek(0)

    return Stream(path, f=f, log=log)


def execute(command):
    logging.debug("execute: %s", command)
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    try:
        outs, errs = proc.communicate(timeout=600)
    except subprocess.TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()

    return proc.returncode, outs.decode("utf-8"), errs.decode("utf-8")


def read_csv(input_path, encoding="utf-8"):
    logging.debug("reading %s with encoding %s", input_path, encoding)
    return open(input_path, encoding=encoding, newline=None)


def read_excel(path):
    try:
        excel = pd.read_excel(path)
    except:  # noqa: E722
        return None

    string = excel.to_csv(
        index=None, header=True, encoding="utf-8", quoting=csv.QUOTE_ALL
    )
    f = StringIO(string)

    return f

def convert_json_to_csv(input_path, output_path=None):
    if not output_path:
        output_path = tempfile.NamedTemporaryFile(suffix=".csv").name
    with open(input_path, "r") as json:
        js = json_stream.load(json)

        columns = None
        data = None

        for item in js.items():
            if item[0] in ["columns"]:
                columns = [x for x in item[1].persistent()]
                if data is not None:
                    save_efficient_json_as_csv(output_path, columns, data)
                    return output_path

            if item[0] in ["data"]:
                if columns is not None:
                    save_efficient_json_as_csv(output_path, columns, item[1])
                    return output_path
                else:
                    data = [x for x in item[1].persistent()]

        return convert_features_to_csv(input_path, output_path)
    
def save_efficient_json_as_csv(output_path, columns, data):
    with open(output_path, "w") as csv_file:
        cw = csv.writer(csv_file)
        cw.writerow(columns)

        for row in data:
            cw.writerow(row)

def convert_features_to_csv(input_path,custom_output_path = None):
    if custom_output_path:
        output_path = custom_output_path
    else:
        output_path = tempfile.NamedTemporaryFile(suffix=".csv").name

    execute(
        [
            "ogr2ogr",
            "-oo",
            "DOWNLOAD_SCHEMA=NO",
            "-lco",
            "GEOMETRY=AS_WKT",
            "-lco",
            "LINEFORMAT=CRLF",
            "-f",
            "CSV",
            "-nlt",
            "MULTIPOLYGON",
            "-nln",
            "MERGED",
            "--config",
            "OGR_WKT_PRECISION",
            "10",
            output_path,
            input_path,
        ]
    )
    if not os.path.isfile(output_path):
        return None

    return output_path


class ConvertPhase(Phase):
    def __init__(self, path=None, dataset_resource_log=None, custom_temp_dir=None,custom_temp_file=None):
        self.path = path
        self.log = dataset_resource_log
        self.charset = ""
        # Allows for custom temporary directory to be specified
        # This allows symlink creation in case of /tmp & path being on different partitions
        if custom_temp_dir:
            self.custom_temp_dir = custom_temp_dir
            self.temp_file_extra_kwargs = {"dir": custom_temp_dir}
        else:
            self.temp_file_extra_kwargs = {}
        # custom_temp_file
        # allows a specified file if user wants to interrogate them
        if custom_temp_file:
            self.custom_temp_file = custom_temp_file

    def process(self, stream=None):
        input_path = self.path

        reader = self._read_binary_file(input_path)

        if not reader:
            encoding = detect_file_encoding(input_path)
            if encoding:
                logging.debug("encoding detected: %s", encoding)
                self.charset = ";charset=" + encoding
                reader = self._read_text_file(input_path, encoding)

        if not reader:
            logging.debug("failed to create reader, cannot process %s", input_path)

            # raise StopIteration()
            reader = iter(())

        return Stream(input_path, f=reader, log=self.log)

    def _read_text_file(self, input_path, encoding):
        f = read_csv(input_path, encoding)
        self.log.mime_type = "text/csv" + self.charset
        content = f.read(10)
        f.seek(0)
        converted_csv_file = None
        if self.custom_temp_file:
                if self.custom_temp_dir:
                    output_file_path = os.path.join(self.custom_temp_dir,self.custom_temp_file)
                else:
                    output_file_path = None

        if content.lower().startswith("<!doctype "):
            self.log.mime_type = "text/html" + self.charset
            logging.warn("%s has <!doctype, IGNORING!", input_path)
            f.close()
            return None

        elif content.lower().startswith(("<?xml ", "<wfs:")):
            logging.debug("%s looks like xml", input_path)
            self.log.mime_type = "application/xml" + self.charset
            if self.custom_temp_file:
                if self.custom_temp_dir:
                    output_file_path = os.path.join(self.custom_temp_dir,self.custom_temp_file)
                else:
                    output_file_path = None
            converted_csv_file = convert_features_to_csv(input_path,output_file_path)
            if not converted_csv_file:
                f.close()
                logging.warning("conversion from XML to CSV failed")
                return None

        elif content.lower().startswith("{"):
            logging.debug("%s looks like json", input_path)
            self.log.mime_type = "application/json" + self.charset
            converted_csv_file = convert_json_to_csv(input_path,output_file_path)

        if converted_csv_file:
            f.close()
            reader = read_csv(converted_csv_file)
        else:
            reader = f

        return reader

    def _find_zip_file(self, input_file, suffix=".gml"):
        zip_ = zipfile.ZipFile(input_file)
        files = zip_.namelist()
        files = list(
            set(
                filter(
                    lambda s: s.endswith(suffix) or s.endswith(suffix.upper()), files
                )
            )
        )
        if not files or not len(files):
            return None
        if len(files) > 1:
            raise ValueError("Zipfile contains more than one %s file" % suffix)
        return "/" + files[0]

    def find_internal_path(self, input_path):
        internal_path = self._find_zip_file(input_path, ".shp")
        if internal_path:
            return internal_path, "x-gis/x-shapefile"

        internal_path = self._find_zip_file(input_path, ".gml")
        if internal_path:
            return internal_path, "application/gml+xml"

        internal_path = self._find_zip_file(input_path, ".tab")
        if internal_path:
            return internal_path, "x-gis/x-mapinfo-tab"

        internal_path = self._find_zip_file(input_path, ".geojson")
        if internal_path:
            return internal_path, "application/vnd.geo+json"

        internal_path = self._find_zip_file(input_path, ".json")
        if internal_path:
            return internal_path, "application/vnd.geo+json"

        internal_path = self._find_zip_file(input_path, ".kml")
        if internal_path:
            return internal_path, "application/vnd.google-earth.kml+xml"

        return None, None

    def _read_binary_file(self, input_path):
        if self.custom_temp_file:
                if self.custom_temp_dir:
                    output_file_path = os.path.join(self.custom_temp_dir,self.custom_temp_file)
                else:
                    output_file_path = None
        # First try excel
        excel_reader = read_excel(input_path)
        if excel_reader:
            logging.debug(f"{input_path} looks like excel")
            self.log.mime_type = "application/vnd.ms-excel"
            return excel_reader

        # Then try zip
        if zipfile.is_zipfile(input_path):
            logging.debug(f"{input_path} looks like zip")
            self.log.mime_type = "application/zip"

            internal_path, mime_type = self.find_internal_path(input_path)
            if internal_path:
                self.log.internal_path = internal_path
                self.log.internal_mime_type = mime_type
                temp_path = tempfile.NamedTemporaryFile(
                    suffix=".zip", **self.temp_file_extra_kwargs
                ).name
                os.link(input_path, temp_path)
                zip_path = f"/vsizip/{temp_path}{internal_path}"
                logging.debug(f"zip_path: {zip_path} mime_type: {mime_type}")
                csv_path = convert_features_to_csv(zip_path,output_file_path)
                encoding = detect_file_encoding(csv_path)
                return read_csv(csv_path, encoding)

        # Then try SQLite (GeoPackage)
        try:
            conn = sqlite3.connect(input_path)
            cursor = conn.cursor()
            cursor.execute("pragma quick_check")
        except:  # noqa: E722
            pass
        else:
            logging.debug(f"{input_path} looks like SQLite")
            self.log.mime_type = "application/geopackage+sqlite3"
            csv_path = convert_features_to_csv(input_path,output_file_path)
            encoding = detect_file_encoding(csv_path)
            return read_csv(csv_path, encoding)

        return None

def download_resource(files_url,resource,collection,output_dir_path):
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
        
    urllib.request.urlretrieve(os.path.join(files_url,collection,'collection','resource',resource),os.path.join(output_dir_path,resource))


def convert_resource(resource,input_path,output_dir,dataset):
    dataset_resource_log = DatasetResourceLog(dataset=dataset, resource=resource)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir,exist_ok=True)
    cp = ConvertPhase(
                    path=input_path,
                    dataset_resource_log=dataset_resource_log,
                    custom_temp_dir=output_dir,
                    custom_temp_file=f'{resource}.csv'
                )

    cp.process()