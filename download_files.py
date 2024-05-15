import requests
from requests.auth import HTTPBasicAuth
from pathlib import Path
import logging
import sys
from math import ceil
from argparse import ArgumentParser
import re

from urllib.parse import urlencode

from utils import cast_logging_level

from typing import Union, List, Dict, Tuple


class CaptureFileDownloader:
    def __init__(
            self,
            address: str,
            username: str,
            password: str,
            download_dir: Union[str, Path] = None,
    ):

        self.address = f"https://{address}/amw4analysis"
        self.address_api = self.address + "/webapi/v1"
        # authenticate with username + password as a valid member of the group "awmcapture"
        self.__credentials = HTTPBasicAuth(username=username, password=password)

        self.default_headers = {"accept": "application/json", "content-type": "application/json"}
        self.verify_ssl = False

        self.download_dir = Path().cwd() if download_dir is None else Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def _get(self, url: str) -> Union[Dict, None]:
        req = requests.get(url, auth=self.__credentials, headers=self.default_headers, verify=self.verify_ssl)
        self._check_status_code(req, url)
        return req.json()

    def _get_all_pages(self, url, size: int = 20) -> List[dict]:
        info = self._get(url + "?size=1")

        n = info["pageMeta"]["totalElements"]
        data = []
        for i in range(0, ceil(n/size)):
            info = self._get(url + f"?size={size}&page={i}")
            data += info["data"]
        assert len(data) == n
        return data

    @staticmethod
    def _check_status_code(req: requests.models.Response, msg: str = "") -> bool:
        if req.status_code == 200:
            logging.info(" ".join([msg, "Success"]))
            return True
        elif req.status_code == 400:
            logging.error(" ".join([msg, "Required parameters are not provided"]))
        elif req.status_code == 401:
            logging.error(" ".join([msg, "Client authorization required"]))
        elif req.status_code == 404:
            logging.error(" ".join([msg, "Specified job or job run does not exist"]))
        else:
            logging.error(" ".join([msg, f"Unexpected status code: {req.status_code}"]))
        return False

    def get_jobs(self) -> Union[List[dict], None]:
        url = f"{self.address_api}/jobs/"
        return self._get_all_pages(url)

    def get_job_ids(self) -> List[str]:
        jobs = self.get_jobs()

        job_ids = []
        for job in jobs:
            # if job was executed
            if job["numberOfRuns"] > 0:
                job_ids.append(job["id"])
        return job_ids

    def get_runs(self, job_id: str) -> Union[List[dict], None]:
        url = f"{self.address_api}/jobs/{job_id}/runs"
        return self._get_all_pages(url)

    def get_run_ids(self, job_id: str) -> List[str]:
        runs = self.get_runs(job_id)

        run_ids = []
        for run in runs:
            run_ids.append(run["jobRunId"])
        return run_ids

    def get_file_infos(self, job_id: str, run_id: str) -> List[dict]:
        url = f"{self.address_api}/jobs/{job_id}/runs/{run_id}/files"
        return self._get_all_pages(url)

    def get_file_names(self, job_id: str, run_id: str) -> List[str]:
        files = self.get_file_infos(job_id, run_id)

        filenames = []
        for fl in files:
            filenames.append(fl["fileName"])
        return filenames

    def download_file(
            self,
            job_id: str,
            run_id: str,
            filename: str,
            delete_at_success: bool = False
    ) -> Union[Path, None]:
        # build url
        url = f"{self.address_api}/jobs/{job_id}/runs/{run_id}/files/{filename}"

        file_path = self.download_dir / filename
        # download file as stream
        flag = download_file(url, file_path, auth=self.__credentials, verify=self.verify_ssl)

        if flag:
            if delete_at_success:
                self.delete_file(job_id, run_id)
            return file_path
        else:
            return None

    def delete_file(self, job_id: str, run_id: str) -> None:
        # build url
        url = f"{self.address_api}/jobs/{job_id}/runs/{run_id}"

        # req = requests.delete(url, auth=self.__credentials, headers=self.default_headers, verify=self.verify_ssl)
        # self._check_status_code(req, url)
        # return req.json()

    def download_files(self, delete_downloaded_files: bool = False) -> List[Path]:
        job_ids = self.get_job_ids()

        files = []
        for job_id in job_ids:
            run_ids = self.get_run_ids(job_id)

            for run_id in run_ids:
                filenames = self.get_file_names(job_id, run_id)

                files.append((job_id, run_id, filenames))
                for file_name in filenames:
                    # download file
                    file = self.download_file(job_id, run_id, file_name, delete_at_success=delete_downloaded_files)
                    files.append(file)
        return files


def download_file(url: str, filename: Union[str, Path], **kwargs) -> bool:
    try:
        with requests.get(url, stream=True, **kwargs) as req:
            req.raise_for_status()
            with open(filename, 'wb') as fid:
                for chunk in req.iter_content(chunk_size=8192):
                    fid.write(chunk)
        return True
    except:
        return False


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--destination", type=str, default="",
                        help="Directory to store the downloaded files in.")

    parser.add_argument("--address", type=str, help="IP address of the Sinumerik Edge")
    parser.add_argument("--username", type=str, help="User name to authenticate at the Capture app.")
    parser.add_argument("--password", type=str, help="Corresponding password for authentication at the Capture app.")

    parser.add_argument("--logging-level", type=str, default="INFO", help="Logging level")

    opt = parser.parse_args()

    # check destination directory
    download_dir = Path(opt.destination)
    if not download_dir.exists() or not download_dir.is_dir():
        raise Exception(f"Destination must be an existing directory. ({download_dir.as_posix()})")

    # check address
    re_ip_address = re.compile("(\d{1,3}\.){3}\d{1,3}(:\d+)?$")
    re_port = re.compile("(?<=:)\d+$")
    if re_ip_address.match(opt.address):
        if not re_port.search(opt.address):
            # add default port
            opt.address += ":5443"

    # setup logging
    logging.basicConfig(
        level=cast_logging_level(opt.logging_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            # logging.FileHandler(Path(get_env_variable("LOGFILE", "log")).with_suffix(".log")),
            logging.StreamHandler(sys.stdout)
        ],
    )

    downloader = CaptureFileDownloader(
        address=opt.address,
        username=opt.username,
        password=opt.password,
        download_dir=download_dir
    )

    downloader.download_files()
