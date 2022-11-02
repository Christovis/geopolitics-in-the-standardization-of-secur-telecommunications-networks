import os
import re
import time
from functools import partial
from typing import List, Dict
from pathlib import Path
import argparse
from tqdm import tqdm
from itertools import compress
from collections import defaultdict
import pandas as pd
import numpy as np
import textract
from collections import namedtuple

from bigbang.analysis.listserv import ListservMailList

from tgpp.config.config import CONFIG
from tgpp.ingress import TextFile
import tgpp.ingress.queries as Queries
import tgpp.nlp.utils as NLPutils

parser = argparse.ArgumentParser(
    description="Find target set within search set of documents.",
)
parser.add_argument(
    "--search_set",
    const=CONFIG.search_set,
    default=CONFIG.search_set,
    type=str,
    nargs='?',
    help='Define the search set which can be [emails]',
)
args = parser.parse_args()


def search_keyterms(
    filepath: str,
    queries: List[str],
    text_preprocessing,
) -> list:
    tset = {}
    text = textract.process(filepath).decode("utf-8")
    # get texts of message body and attachment
    # preprocess texts
    preproc_text = text_preprocessing(text, return_tokens=False)
    # add query counts
    for query in queries:
        tset[f'{query}'] = preproc_text.count(query)
    # add extra fields
    tset['filepath'] = filepath.split('/')[-1]
    tset['token_count'] = len(preproc_text)
    return list(tset.values())


if __name__ == "__main__":
    # load keyterms/queries
    queries = Queries.load_abbreviations(CONFIG.file_queries)
    queries = [q for q in queries if isinstance(q, str)]

    queries = NLPutils.text_preprocessing(
        queries,
        min_len=1,
        max_len=30,
        keep_nonalphanumerics=True,
        remove_numbers=False,
        return_tokens=True,
    )
    # remove dublicates
    queries = list(np.unique(queries))
    # remove empty strings
    if '' in queries:
        queries.remove('')
    # padding with white space to avoid unwanted term embeddings
    queries = [' '+query+' ' for query in queries]
    # fix settings for text processing
    min_term_len = np.min([len(term) for query in queries for term in query.strip().split(' ')])
    max_term_len = np.max([len(term) for query in queries for term in query.strip().split(' ')])
    
    remove_numbers = NLPutils.contains_digits(queries)
    non_alphanumerics = NLPutils.return_non_alphanumerics(queries)
    non_alphanumerics = ["\\"+na for na in non_alphanumerics]

    print("---------------------------")
    print(min_term_len, max_term_len)
    print(non_alphanumerics, remove_numbers)
    print("---------------------------")

    text_preprocessing = partial(
        NLPutils.text_preprocessing,
        min_len=min_term_len,
        max_len=max_term_len,
        keep_nonalphanumerics=non_alphanumerics,
        remove_numbers=False,
    )

    # load mailinglist, S-set
    folder_docs = "/home/christovis/InternetGovernance/bigbang/archives/MeetingReports3GPP/"
    filepaths_docs = [folder_docs + filename for filename in os.listdir(folder_docs)]
    print(f"There are {len(filepaths_docs)} meeting reports.")

    # run through messages and count keyterms
    tset = []
    for filepath_doc in filepaths_docs:
        if filepath_doc.lower().split('.')[-1] not in ['zip', 'xml', 'rtf', 'xlsm']:
            try:
                tset.append(search_keyterms(filepath_doc, queries, text_preprocessing))
            except Exception:
                continue

    # Target-set Email attributes
    attributes = {}
    for query in queries:
        attributes[f'{query}'] = int
    attributes['filepath'] = str
    attributes['token_count'] = int

    tset = np.asarray(tset).T
    tset_dic = {}
    for idx, (attribute, datatype) in enumerate(attributes.items()):
        tset_dic[attribute] = tset[idx].astype(datatype)

    df = pd.DataFrame.from_dict(tset_dic)
    df = Queries.remove_text_wo_query(df)
    df = Queries.remove_query_wo_text(df)
    print(len(df.index), len(df.columns))

    #TODO: Need to specify escapechar as white-space etc can be contianed in text body
    #df.to_csv(_file_path, escapechar=), hdf5 might therefore be better in that case
    #_file_path = CONFIG.folder_target_set + f"{args.search_set}.h5"
    _file_path = CONFIG.folder_target_set + "MeetingReports3GPP.h5"
    df.to_hdf(_file_path, key='df', mode='w')
