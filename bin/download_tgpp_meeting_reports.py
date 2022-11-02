import os
import tempfile
from pathlib import Path
from unittest import mock
import shutil
import gzip
import requests

import pytest
import yaml

import bigbang
from bigbang.ingress import (
    ThreeGPPWGArchive,
)
from config.config import CONFIG

url_3gppwgarchive = "https://www.3gpp.org/ftp/tsg_sa/WG3_Security"


#marchive = ThreeGPPWGArchive.from_url(
#    name="TSG_SA",
#    url=url_3gppwgarchive,
#    select={
#        "file_extensions": ["docx", "txt"],
#    },
#    doc_limit=2000,
#)
#marchive.to_txt_file("../data/search_set/threegpp_meetingreports_urls.txt")

with open("../data/search_set/MeetingReports.3GPP.txt", 'r') as fp:
    doc_urls = [line.replace('\n', '') for line in fp.readlines()]

print(doc_urls)
marchive = ThreeGPPWGArchive.from_doc_urls(
    name="TSG_SA",
    url=url_3gppwgarchive,
    doc_urls=doc_urls,
)
marchive.download_docs()
marchive.unzip_docs()
